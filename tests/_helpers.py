from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from sbit.config import Config


@dataclass(frozen=True)
class FeedFixture:
    """Maps a local fixture file to its landing-zone target."""

    local_pattern: str  # e.g. "registered_users_{set}.csv"
    target_dir: str  # subdir under landing_zone (e.g. "registered_users_bz")

    def local_path(self, local_root: Path, set_num: int) -> Path:
        return local_root / self.local_pattern.format(set=set_num)

    def target_path(self, landing_zone: str, set_num: int) -> str:
        return f'{landing_zone}/{self.target_dir}/{self.local_pattern.format(set=set_num)}'


FEED_FIXTURES: tuple[FeedFixture, ...] = (
    FeedFixture('registered_users_{set}.csv', 'registered_users_bz'),
    FeedFixture('user_info_{set}.json', 'kafka_multiplex_bz'),
    FeedFixture('workout_{set}.json', 'kafka_multiplex_bz'),
    FeedFixture('bpm_{set}.json', 'kafka_multiplex_bz'),
    FeedFixture('gym_logins_{set}.csv', 'gym_logins_bz'),
)

AVAILABLE_SETS = (1, 2)


class DataProducer:
    """Helper class to produce data for tests."""

    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        *,
        local_data_root: Path,
        workspace_client: WorkspaceClient | None = None,
        feeds: tuple[FeedFixture, ...] = FEED_FIXTURES,
        available_sets: tuple[int, ...] = AVAILABLE_SETS,
    ) -> None:
        self.spark = spark
        self.config = config
        self.landing_zone_path = (
            f'/Volumes/{config.catalogue_name}/{config.db_name}/{config.volume_name}/{config.base_data_path}'
        )
        self.local_data_root = local_data_root
        self._workspace_client = workspace_client
        self.feeds = feeds

    @property
    def w(self) -> WorkspaceClient:
        if self._workspace_client is None:
            self._workspace_client = WorkspaceClient()
        return self._workspace_client

    def _upload(self, feed: FeedFixture, set_num: int) -> None:
        source = feed.local_path(self.local_data_root, set_num)
        target = feed.target_path(self.landing_zone_path, set_num)
        print(f'Producing {source} -> {target}')
        self.w.files.upload_from(target, str(source), overwrite=True)

    def produce(self, set_num: int) -> None:
        if set_num not in AVAILABLE_SETS:
            available = ', '.join(str(s) for s in AVAILABLE_SETS)
            raise ValueError(f'set_num={set_num} not in available sets: [{available}]')
        print(f'Producing test data set {set_num}')
        for feed in self.feeds:
            self._upload(feed, set_num)
        print(f'Test data set {set_num} produced successfully.')
