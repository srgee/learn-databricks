from pathlib import Path

import pytest
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from sbit.config import Config, load_settings


@pytest.fixture(scope='session')
def spark():
    spark = DatabricksSession.builder.getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope='session')
def config() -> Config:
    return load_settings([])


@pytest.fixture(scope='session')
def setup_date_lookup_file(spark: SparkSession, cfg: Config) -> str:
    """Ensure Unity Catalog schema and volume exist, then upload date-lookup.json."""
    spark.sql(f'CREATE CATALOG IF NOT EXISTS `{cfg.catalogue_name}`')
    spark.sql(f'CREATE DATABASE IF NOT EXISTS `{cfg.catalogue_name}`.`{cfg.db_name}`')
    spark.sql(f'CREATE VOLUME IF NOT EXISTS `{cfg.catalogue_name}`.`{cfg.db_name}`.`{cfg.volume_name}`')

    remote_path = '/Volumes/dev/db/vol_name/raw/date-lookup.json'
    local_path = Path(__file__).parent.parent / 'data' / 'date-lookup.json'

    w = WorkspaceClient()
    with open(local_path, 'rb') as f:
        w.files.upload(remote_path, f, overwrite=True)

    return remote_path


@pytest.fixture
def wearable_sample_data():
    """Test data for pipelines and transformations."""
    return [
        {
            'device_id': 'wearable-001',
            'timestamp': '2026-10-22T08:00:00Z',
            'heart_rate': 72,
            'steps': 0,
            'accelerometer': (0.12, -0.05, 9.81),
        },
        {
            'device_id': 'wearable-001',
            'timestamp': '2026-10-22T08:01:00Z',
            'heart_rate': 78,
            'steps': 12,
            'accelerometer': (0.31, -0.18, 9.76),
        },
        {
            'device_id': 'wearable-001',
            'timestamp': '2026-10-22T08:02:00Z',
            'heart_rate': 81,
            'steps': 28,
            'accelerometer': (0.45, -0.22, 9.70),
        },
        {
            'device_id': 'wearable-002',
            'timestamp': '2026-10-22T08:00:00Z',
            'heart_rate': 65,
            'steps': 0,
            'accelerometer': (0.08, 0.01, 9.80),
        },
        {
            'device_id': 'wearable-002',
            'timestamp': '2026-10-22T08:01:00Z',
            'heart_rate': 70,
            'steps': 5,
            'accelerometer': (0.20, -0.10, 9.77),
        },
    ]
