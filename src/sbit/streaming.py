from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from sbit.upserters import MergeSpec, merge_batch


@dataclass(frozen=True)
class StreamSpec:
    name: str  # Target table (short name)
    source: str  # Source table (short name)
    merge: MergeSpec
    transform: Callable[[DataFrame], DataFrame] | None = None
    batch_transform: Callable[[DataFrame], DataFrame] | None = None
    dedupe_keys: tuple[str, ...] = ()
    watermark_col: str | None = None
    watermark_delay: str = '30 seconds'
    ignore_changes: bool = False  # Ignore changes if source is a Silver table
    output_mode: str = 'update'
    scheduler_pool: str = 'silver_p3'
    build_df: Callable[[SparkSession, int], DataFrame] | None = None


class UpsertPipeline:
    def __init__(self, spark: SparkSession, checkpoint_root: str, starting_version: int = 0) -> None:
        self.spark = spark
        self.checkpoint_root = checkpoint_root
        self.starting_version = starting_version

    def _source_options(self, s: StreamSpec) -> dict:
        key = 'ignoreChanges' if s.ignore_changes else 'ignoreDeletes'
        return {'startingVersion': self.starting_version, key: 'true'}

    def _build_input_df(self, s: StreamSpec) -> DataFrame:
        """Build streaming input: custom build function or read from table (standard)."""
        if s.build_df is not None:
            return s.build_df(self.spark, self.starting_version)
        df = self.spark.readStream.options(**self._source_options(s)).table(s.source)
        return s.transform(df) if s.transform is not None else df

    def start(self, s: StreamSpec, once: bool = True, processing_time: str = '15 seconds') -> StreamingQuery:
        df = self._build_input_df(s)
        if s.watermark_col:
            df = df.withWatermark(s.watermark_col, s.watermark_delay)
        if s.dedupe_keys:
            df = df.dropDuplicates(list(s.dedupe_keys))

        self.spark.sparkContext.setLocalProperty('spark.scheduler.pool', s.scheduler_pool)
        upsert = merge_batch(self.spark, s.name, s.merge, batch_transform=s.batch_transform)
        writer = (
            df.writeStream.foreachBatch(upsert)
            .outputMode(s.output_mode)
            .option('checkpointLocation', f'{self.checkpoint_root}/silver_{s.name}')
            .queryName(f'{s.name}_upsert_stream')
        )
        writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=processing_time)

        return writer.start()

    def run(
        self,
        specs: dict[str, StreamSpec],
        phases: list[list[str]],
        once: bool = True,
        processing_time: str = '15 seconds',
    ):
        for i, phase in enumerate(phases, 1):
            queries = [self.start(specs[name], once, processing_time) for name in phase]
            if once:
                for q in queries:
                    q.awaitTermination()
