from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from sbit.bronze_specs import SourceFeed


class BronzeIngester:
    """Streaming engine that lands raw files into Bronze Delta tables.

    Reads each source feed declared in ``sbit.bronze.FEEDS`` via Auto
    Loader (``cloudFiles``), applies the per-feed schema and transform
    (typically ``add_audit_columns`` to attach ingestion metadata), and
    writes the result to a Bronze Delta table named after the feed. Each
    feed is ingested independently with its own checkpoint, so a failure
    in one does not block the others.

    In streaming mode, runs as a continuous Spark Structured Streaming job
    with the configured ``processing_time`` trigger; in batch mode, processes the
    currently available files once and exits. ``maxFilesPerTrigger`` (declared
    per feed in ``SourceFeed.options``) throttles ingestion to avoid OOM on the cluster.

    Attributes:
        spark: SparkSession used to define and start the streams.
        source_path: Volume path where raw files arrive.
        checkpoint_path: Volume path where per-feed stream
            offsets are persisted for exactly-once semantics.
    """

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
