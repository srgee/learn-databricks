import pytest
from pyspark.sql import SparkSession

from wearpipe.bronze import FEEDS
from wearpipe.config import Config
from wearpipe.gold_specs import GOLD_STREAMS
from wearpipe.setup import SetupHelper
from wearpipe.silver_specs import SILVER_STREAMS


@pytest.fixture(scope='session')
def setup_database(spark: SparkSession, config: Config):
    """Fixture to run database setup and ensure clean teardown after tests."""
    helper = SetupHelper(
        spark=spark,
        catalogue_name=config.catalogue_name,
        db_name=config.db_name,
        volume_name=config.volume_name,
        data_root=config.base_data_path,
    )
    helper.setup_db()
    yield helper
    helper.cleanup_db()


# ============================================================
# Helpers: derive expected tables from specs
# ============================================================


def expected_bronze_tables() -> list[str]:
    return [feed.name for feed in FEEDS]


def expected_silver_tables() -> list[str]:
    return [spec.name for spec in SILVER_STREAMS.values()]


def expected_gold_tables() -> list[str]:
    return [spec.name for spec in GOLD_STREAMS.values()]


def expected_other_tables() -> list[str]:
    """Tables not driven by specs: date_lookup is loaded from a static file."""
    return ['date_lookup']


def expected_views() -> list[str]:
    """Views are SQL-defined and not spec-driven (gym_summary is the only one)."""
    return ['gym_summary']


def all_expected_tables() -> list[str]:
    """Union of all expected physical tables (bronze + silver + gold + other)."""
    return expected_bronze_tables() + expected_silver_tables() + expected_gold_tables() + expected_other_tables()


def test_database_exists(spark: SparkSession, config: Config, setup_database):
    """Verify that the Unity Catalog database exists."""
    db_fqn = f'{config.catalogue_name}.{config.db_name}'

    assert spark.catalog.databaseExists(db_fqn)


@pytest.mark.parametrize('table_name', all_expected_tables())
def test_table_exists(spark: SparkSession, config: Config, setup_database, table_name):
    """Each expected table must exist after setup_db()."""
    fqn = f'{config.catalogue_name}.{config.db_name}.{table_name}'
    assert spark.catalog.tableExists(fqn), (
        f"Table '{fqn}' was not created by setup_db(). "
        f'Check that the corresponding spec is registered in '
        f'SILVER_STREAMS / GOLD_STREAMS / FEEDS, or that the table '
        f'is listed in expected_other_tables() if it is not spec-driven.'
    )


def test_bronze_table_count_matches_feeds(spark: SparkSession, config: Config, setup_database):
    """Sanity check: number of bronze tables must equal len(FEEDS).

    Catches a regression where a new feed is added to FEEDS but
    BronzeIngester.ingest_all() doesn't pick it up.
    """
    bronze_tables = [
        t.name for t in spark.catalog.listTables(f'{config.catalogue_name}.{config.db_name}') if t.name.endswith('_bz')
    ]
    assert len(bronze_tables) == len(expected_bronze_tables()), (
        f'Bronze table count mismatch: found {bronze_tables}, expected {expected_bronze_tables()}'
    )


@pytest.mark.parametrize('spec_name', list(SILVER_STREAMS.keys()))
def test_silver_table_schema_matches_spec(spark: SparkSession, config: Config, setup_database, spec_name):
    spec = SILVER_STREAMS[spec_name]
    fqn = f'{config.catalogue_name}.{config.db_name}.{spec.name}'
    actual_fields = {f.name for f in spark.read.table(fqn).schema.fields}
    expected_fields = {f.name for f in spec.target_schema.fields}
    assert actual_fields == expected_fields, (
        f"Silver table '{fqn}' has wrong schema. "
        f'expected={sorted(expected_fields)}, actual={sorted(actual_fields)}. '
        f'Check that ddl_for_spec() is generating backtick-quoted DDL.'
    )


@pytest.mark.parametrize('spec_name', list(GOLD_STREAMS.keys()))
def test_gold_table_schema_matches_spec(spark: SparkSession, config: Config, setup_database, spec_name):
    spec = GOLD_STREAMS[spec_name]
    fqn = f'{config.catalogue_name}.{config.db_name}.{spec.name}'
    actual_fields = {f.name for f in spark.read.table(fqn).schema.fields}
    expected_fields = {f.name for f in spec.target_schema.fields}
    assert actual_fields == expected_fields, (
        f"Gold table '{fqn}' has wrong schema. expected={sorted(expected_fields)}, actual={sorted(actual_fields)}."
    )


@pytest.mark.parametrize('view_name', expected_views())
def test_view_exists_and_is_queryable(spark: SparkSession, config: Config, setup_database, view_name):
    """Each view must exist and be queryable (no AnalysisException).

    Querying the view forces Spark to resolve its definition (joins,
    columns, types). If the view references a non-existent table or
    has a SQL error, this test surfaces it instead of letting it
    blow up at runtime in production.
    """
    fqn = f'{config.catalogue_name}.{config.db_name}.{view_name}'
    assert spark.catalog.tableExists(fqn), f"View '{fqn}' does not exist"

    # Force schema resolution: collect() would also work but count()
    # is cheaper for views that might be empty.
    spark.read.table(fqn).schema  # noqa: B018 — accessing schema forces resolution
    # If we got here without AnalysisException, the view is valid.


def test_gym_summary_view_has_expected_columns(spark: SparkSession, config: Config, setup_database):
    """gym_summary view must expose the documented columns.

    Catches drift between the view's SQL definition and its consumers
    (any downstream code that reads gym_summary).
    """
    fqn = f'{config.catalogue_name}.{config.db_name}.gym_summary'
    actual_cols = {f.name for f in spark.read.table(fqn).schema.fields}
    # These are the columns defined in CREATE OR REPLACE VIEW gym_summary
    expected_cols = {
        'date',
        'gym',
        'mac_address',
        'workout_id',
        'session_id',
        'minutes_in_gym',
        'minutes_exercising',
    }
    assert actual_cols == expected_cols, (
        f'gym_summary view has wrong columns. expected={sorted(expected_cols)}, actual={sorted(actual_cols)}.'
    )
