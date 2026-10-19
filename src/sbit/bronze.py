from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from sbit.bronze_specs import SourceFeed


class BronzeIngester:
    def __init__(self, spark: SparkSession, base_dir: str, checkpoint_path: str):
        self.spark = spark
        self.base_dir = base_dir
        self.checkpoint_path = checkpoint_path

    def ingest(self, feed: SourceFeed, once: bool = True, processing_time: str | None = None) -> StreamingQuery:
        df_stream = (
            self.spark.readStream.format('cloudFiles')
            .schema(feed.schema)
            .option('cloudFiles.format', feed.file_format)
            .options(**feed.options)
            .load(f'{self.base_dir}/{feed.name}')
        )

        if feed.transform is not None:
            df_stream = feed.transform(df_stream)

        writer = (
            df_stream.writeStream.format('delta')
            .option('checkpointLocation', f'{self.checkpoint_path}/{feed.name}')
            .outputMode('append')
            .queryName(f'{feed.name}_ingestion')
        )
        writer = (
            writer.trigger(availableNow=True) if once else writer.trigger(processingTime=processing_time or '5 seconds')
        )
        return writer.toTable(feed.name)

    def ingest_all(
        self, feeds: list[SourceFeed], once: bool = True, processing_time: str | None = None
    ) -> list[StreamingQuery]:
        queries = [self.ingest(f, once, processing_time) for f in feeds]
        if once:
            for q in queries:
                q.awaitTermination()
        return queries
