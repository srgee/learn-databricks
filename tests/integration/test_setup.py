import pytest
from pyspark.sql import SparkSession
from wearpipe.config import Config
from wearpipe.setup import SetupHelper


@pytest.fixture(scope='session')
def setup_database(spark: SparkSession, config: Config):
    """Fixture to run database setup and ensure clean teardown after tests."""
    helper = SetupHelper(spark, config.catalogue_name, config.db_name, config.volume_name)
    helper.setup_db()
    yield helper
    helper.cleanup_db()


def test_database_creation(spark: SparkSession, config: Config, setup_database):
    """Verify that the Unity Catalog database exists."""
    assert spark.catalog.databaseExists(f'`{config.catalogue_name}`.`{config.db_name}`')


def test_all_tables_and_views_exist(spark: SparkSession, config: Config, setup_database):
    """Verify that every table and view defined in SetupHelper is successfully created."""
    helper = setup_database

    for table_name in helper.tables:
        fully_qualified_name = f'`{config.catalogue_name}`.`{config.db_name}`.`{table_name}`'
        assert spark.catalog.tableExists(fully_qualified_name), (
            f"La tabla o vista requerida '{table_name}' no existe en {config.catalogue_name}.{config.db_name}"
        )


def test_gym_summary_view_definition(spark: SparkSession, config: Config, setup_database):
    """Verify that the gym_summary view can be queried successfully."""
    view_name = f'`{config.catalogue_name}`.`{config.db_name}`.`gym_summary`'

    # Comprobar que la vista es accesible (aunque esté vacía inicialmente)
    df = spark.read.table(view_name)
    assert df is not None
