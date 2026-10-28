from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery
from wearpipe.transforms import add_audit_columns, join_date_lookup


@dataclass(frozen=True)
class SourceFeed:
    name: str
    file_format: str
    schema: str
    options: dict[str, str] = field(default_factory=dict)
    transform: Callable[[DataFrame], DataFrame] | None = field(default=None)


FEEDS = [
    SourceFeed(
        name='registered_users_bz',
        file_format='csv',
        schema='user_id long, device_id long, mac_address string, registration_timestamp double',
        transform=add_audit_columns,
        options={'header': 'true', 'maxFilesperTrigger': '1'},
    ),
    SourceFeed(
        name='gym_logins_bz',
        file_format='csv',
        schema='mac_address string, gym bigint, login double, logout double',
        options={'header': 'true', 'maxFilesPerTrigger': '1'},
        transform=add_audit_columns,
    ),
    SourceFeed(
        name='kafka_multiplex_bz',
        file_format='json',
        schema='key string, value string, topic string, partition bigint, offset bigint, timestamp bigint',
        options={'maxFilesPerTrigger': '1'},
        transform=lambda df: join_date_lookup(add_audit_columns(df)),
    ),
]


class BronzeIngester:
    """Streaming engine that lands raw files into Bronze Delta tables.

    Reads each source feed declared in ``wearpipe.bronze.FEEDS`` via Auto
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
