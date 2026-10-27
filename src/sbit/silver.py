"""Silver layer: stream CDF from Bronze, stage changes, MERGE into Silver targets.

Architecture:
    Each Silver target is produced by:
    1. Streaming: read CDF changes from the Bronze source (or build the
       stream via ``build_df`` for stream-stream joins / aggregations),
       apply the spec's transform + watermark + dropDuplicates, append
       to an ephemeral ``{name}_stage`` Delta table.
    2. Batch MERGE: when the stream drains, read the stage as a batch
       DataFrame, apply ``batch_transform`` if present, and MERGE into the
       target table using the DeltaTable API.

This stage + batch-MERGE pattern replaces the original ``foreachBatch``
approach, whose isolated Python worker sandbox failed to start on Databricks
Serverless compute accessed through Spark Connect
(``ISOLATION_STARTUP_FAILURE.SANDBOX_STARTUP``).

The pipeline's public API (``run(specs, phases, once=True)``), checkpoint
locations (``silver_{name}``), watermark, ``dropDuplicates``, and
``batch_transform`` semantics are preserved from the original ``UpsertPipeline``.
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from sbit.specs import SilverSpec


class SilverMerger:
    """Stage + batch-MERGE pipeline for the Silver layer.

    See module docstring for the architecture overview.

    Args:
        spark: Active SparkSession.
        checkpoint_root: UC volume path where stream offsets are persisted
            (e.g. ``/Volumes/cat/db/vol/checkpoint_zone``).
        starting_version: Delta log version to start streaming from
            (``0`` = from table creation, ``"earliest"``, or ``"latest"``).
            Honored only when the checkpoint is empty; on restart the
            checkpoint's stored offset takes precedence.
    """

    def __init__(
        self,
        spark: SparkSession,
        checkpoint_root: str,
        starting_version: int = 0,
    ) -> None:
        self.spark = spark
        self.checkpoint_root = checkpoint_root
        self.starting_version = starting_version

    # ---------- Helpers de fuente ----------

    def _source_options(self, s: SilverSpec) -> dict:
        key = 'ignoreChanges' if s.ignore_changes else 'ignoreDeletes'
        return {'startingVersion': self.starting_version, key: 'true'}

    def _build_input_df(self, s: SilverSpec) -> DataFrame:
        """Build streaming input: custom build function or read from table (standard)."""
        if s.build_df is not None:
            return s.build_df(self.spark, self.starting_version)
        df = self.spark.readStream.options(**self._source_options(s)).table(s.source)
        return s.transform(df) if s.transform is not None else df

    @staticmethod
    def _stage_name(name: str) -> str:
        return f'{name}_stage'

    def _checkpoint_path(self, s: SilverSpec) -> str:
        # 'silver_' hardcoded: this engine only handles silver.
        # GoldBuilder (step 2) will use 'gold_' for its specs.
        return f'{self.checkpoint_root}/silver_{s.name}'

    def start(self, s: SilverSpec, once: bool = True, processing_time: str = '15 seconds') -> StreamingQuery:
        """Start the writing stream that writes to the stage table (no forEach)."""
        df = self._build_input_df(s)
        if s.watermark_col:
            df = df.withWatermark(s.watermark_col, s.watermark_delay)
        if s.dedupe_keys:
            df = df.dropDuplicates(list(s.dedupe_keys))

        stage_table = self._stage_name(s.name)
        checkpoint = self._checkpoint_path(s)

        # If a previous run left the stage table behind (e.g., aborted test),
        # drop it so the stream regenerates it with the current schema.
        # The checkpoint is respected: the stream will only process new changes.
        self.spark.sql(f'DROP TABLE IF EXISTS {stage_table}')

        writer = (
            df.writeStream.format('delta')
            .outputMode('append')
            .option('checkpointLocation', checkpoint)
            .queryName(f'{s.name}_stage_stream')
        )
        writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=processing_time)

        return writer.toTable(stage_table)

    def _merge_stage_to_target(self, s: SilverSpec) -> None:
        """MERGE batch stage with target table (native DeltaTable API)."""
        stage_df = self.spark.read.table(self._stage_name(s.name))
        if s.batch_transform is not None:
            stage_df = s.batch_transform(stage_df)

        # Sanity check: the stage table should have the same schema as the target.
        # Catches inconsistent schema changes.
        target_fields = set(self.spark.read.table(s.name).schema.fieldNames())
        source_fields = set(stage_df.schema.fieldNames())
        if target_fields != source_fields:
            missing = source_fields - target_fields
            extra = target_fields - source_fields
            raise RuntimeError(
                f'[{s.name}] schema mismatch after _ensure_target_exists: '
                f'missing in target={missing}, extra in target={extra}'
            )

        target = DeltaTable.forName(self.spark, s.name)
        builder = target.alias('t').merge(stage_df.alias('s'), s.merge.join_on)

        if s.merge.update:
            update_set = s.merge.update_set
            condition = s.merge.update_condition
            if update_set is None:
                # UPDATE SET *
                builder = (
                    builder.whenMatchedUpdateAll(condition=condition) if condition else builder.whenMatchedUpdateAll()
                )
            else:
                # Narrow UPDATE
                builder = (
                    builder.whenMatchedUpdate(condition=condition, set=update_set)
                    if condition
                    else builder.whenMatchedUpdate(set=update_set)
                )

        builder = builder.whenNotMatchedInsertAll()
        builder.execute()

    def _drop_stage(self, s: SilverSpec) -> None:
        self.spark.sql(f'DROP TABLE IF EXISTS {self._stage_name(s.name)}')

    def run(
        self,
        specs: dict[str, SilverSpec],
        phases: list[list[str]],
        once: bool = True,
        processing_time: str = '15 seconds',
    ) -> None:
        """Orchestration."""
        for phase in phases:
            queries = [self.start(specs[name], once, processing_time) for name in phase]
            if once:
                for q in queries:
                    q.awaitTermination()
            for name in phase:
                self._merge_stage_to_target(specs[name])
                self._drop_stage(specs[name])
