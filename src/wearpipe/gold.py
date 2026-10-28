"""Gold layer: build aggregates from Silver, append to Gold targets.

Architecture:
    Each Gold target is produced by:
    1. Streaming: ``build_df`` constructs the stream from scratch (typically
       stream-stream joins or aggregations over Silver sources), and
       appends to an ephemeral ``{name}_stage`` Delta table.
    2. Batch MERGE: when the stream drains, MERGE the stage into the
       target using the DeltaTable API. MERGE is insert-only by design:
       Gold rows are immutable facts keyed by natural identifiers, so
       re-runs of the same source data converge to the same target state.

This module is the Gold-layer counterpart to ``silver.py``. The two
engines share the stage + batch-MERGE pattern (which replaced the
original ``foreachBatch`` approach for Databricks Serverless
compatibility) but do NOT share a base class: each layer owns its spec
and engine, with only the fields and merge semantics that layer needs.
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType

from wearpipe.specs import GoldSpec


def _verify_schema_match(
    spec_name: str,
    expected_schema: StructType,
    actual_schema: StructType,
) -> None:
    """Verify that an actual DataFrame schema matches the expected spec schema."""
    expected = {f.name: f.dataType.simpleString() for f in expected_schema.fields}
    actual = {f.name: f.dataType.simpleString() for f in actual_schema.fields}

    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)
    type_mismatches = {
        name: f'{expected[name]} -> {actual[name]}'
        for name in (set(expected) & set(actual))
        if expected[name] != actual[name]
    }

    if missing or extra or type_mismatches:
        parts = [f'[{spec_name}] stage schema does not match target_schema:']
        if missing:
            parts.append(f'  missing in stage: {sorted(missing)}')
        if extra:
            parts.append(f'  extra in stage: {sorted(extra)}')
        if type_mismatches:
            parts.append(f'  type mismatches: {type_mismatches}')
        raise RuntimeError('\n'.join(parts))


class GoldBuilder:
    """Stage + batch-MERGE pipeline for the Gold layer."""

    def __init__(
        self,
        spark: SparkSession,
        checkpoint_root: str,
        starting_version: int = 0,
    ) -> None:
        self.spark = spark
        self.checkpoint_root = checkpoint_root
        self.starting_version = starting_version

    @staticmethod
    def _stage_name(name: str) -> str:
        return f'{name}_stage'

    def _checkpoint_path(self, s: GoldSpec) -> str:
        return f'{self.checkpoint_root}/gold_{s.name}'

    def start(self, s: GoldSpec, once: bool = True, processing_time: str = '15 seconds') -> StreamingQuery:
        """Start the stream that writes the build_df output to the stage table."""
        df = s.build_df(self.spark, self.starting_version)

        stage_table = self._stage_name(s.name)
        checkpoint = self._checkpoint_path(s)

        # Drop a leftover stage table from an aborted previous run so the
        # stream regenerates it with the current schema. The checkpoint
        # is respected: the stream only processes new changes.
        self.spark.sql(f'DROP TABLE IF EXISTS {stage_table}')

        writer = (
            df.writeStream.format('delta')
            .outputMode('append')
            .option('checkpointLocation', checkpoint)
            .queryName(f'{s.name}_stage_stream')
        )
        writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=processing_time)

        return writer.toTable(stage_table)

    def _merge_stage_to_target(self, s: GoldSpec) -> None:
        """Insert-only MERGE stage → target (native DeltaTable API).

        Gold rows are immutable: the same natural key always maps to the
        same row content, so matched rows are silently skipped and only
        new rows are inserted. This makes re-runs idempotent without
        needing an update branch.
        """
        stage_df = self.spark.read.table(self._stage_name(s.name))

        # Sanity check: catches schema drifts before merge
        _verify_schema_match(s.name, s.target_schema, stage_df.schema)

        target = DeltaTable.forName(self.spark, s.name)
        target.alias('t').merge(stage_df.alias('s'), s.merge.join_on).whenNotMatchedInsertAll().execute()

    def _drop_stage(self, s: GoldSpec) -> None:
        self.spark.sql(f'DROP TABLE IF EXISTS {self._stage_name(s.name)}')

    def run(
        self,
        specs: dict[str, GoldSpec],
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
