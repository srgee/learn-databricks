from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from sbit.silver_specs import SilverStream
from sbit.upserters import merge_batch


class SilverPipeline:
    def __init__(self, spark: SparkSession, checkpoint_root: str, starting_version: int = 0) -> None:
        self.spark = spark
        self.checkpoint_root = checkpoint_root
        self.starting_version = starting_version

    def _source_options(self, s: SilverStream) -> dict:
        key = 'ignoreChanges' if s.ignore_changes else 'ignoreDeletes'
        return {'startingVersion': self.starting_version, key: 'true'}

    def _build_input_df(self, s: SilverStream) -> DataFrame:
        """Build streaming input: custom build function or read from table (standard)."""
        if s.build_df is not None:
            return s.build_df(self.spark, self.starting_version)
        df = self.spark.readStream.options(**self._source_options(s)).table(s.source)
        return s.transform(df) if s.transform is not None else df

    def start(self, s: SilverStream, once: bool = True, processing_time: str = '15 seconds') -> StreamingQuery:
        df = self._build_input_df(s)
        if s.watermark_col:
            df = df.withWatermark(s.watermark_col, s.watermark_delay)
        if s.dedupe_keys:
            df = df.dropDuplicates(list(s.dedupe_keys))

        self.spark.sparkContext.setLocalProperty('spark.scheduler.pool', s.scheduler_pool)

        writer = (
            df.writeStream.foreachBatch(merge_batch(self.spark, s.name, s.merge))
            .outputMode(s.output_mode)
            .option('checkpointLocation', f'{self.checkpoint_root}/silver_{s.name}')
            .queryName(f'{s.name}_upsert_stream')
        )
        writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=processing_time)

        return writer.start()
