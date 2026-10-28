import pytest
from databricks.connect import DatabricksSession
from wearpipe.config import Config, load_settings


@pytest.fixture(scope='session')
def spark():
    spark = DatabricksSession.builder.getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope='session')
def config() -> Config:
    return load_settings([])
