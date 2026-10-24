from pathlib import Path

import pytest
from _helpers import DataProducer
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
def date_lookup_file(spark: SparkSession, config: Config) -> str:
    spark.sql(f'CREATE CATALOG IF NOT EXISTS `{config.catalogue_name}`')
    spark.sql(f'CREATE DATABASE IF NOT EXISTS `{config.catalogue_name}`.`{config.db_name}`')
    spark.sql(f'CREATE VOLUME IF NOT EXISTS `{config.catalogue_name}`.`{config.db_name}`.`{config.volume_name}`')

    remote_path = f'/Volumes/{config.catalogue_name}/{config.db_name}/{config.volume_name}/{config.base_data_path}/tests/date-lookup.json'  # noqa: E501
    local_path = Path(__file__).parent.parent / 'data' / 'date-lookup.json'

    w = WorkspaceClient()
    w.files.upload_from(remote_path, str(local_path), overwrite=True)

    return remote_path


@pytest.fixture(scope='session')
def data_files(config: Config) -> list[str]:
    """Uploads all files from tests/data/ to the volume and returns
    the list of remote paths (strings)."""
    w = WorkspaceClient()
    local_base = Path(__file__).parent.parent / 'data'
    remote_base = (
        f'/Volumes/{config.catalogue_name}/{config.db_name}/{config.volume_name}/{config.base_data_path}/tests'
    )

    remote_paths: list[str] = []
    if local_base.exists():
        for file_path in local_base.glob('*'):
            if file_path.is_file():
                remote_path = f'{remote_base}/{file_path.name}'
                w.files.upload_from(remote_path, str(file_path), overwrite=True)
                remote_paths.append(remote_path)
    return remote_paths


@pytest.fixture
def data_producer(spark, config):
    data_dir = Path(__file__).parent.parent / 'data'
    return DataProducer(
        spark=spark,
        config=config,
        local_data_root=data_dir,
    )
