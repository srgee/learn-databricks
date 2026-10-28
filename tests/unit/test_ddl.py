"""Unit tests for wearpipe.ddl.ddl_for_spec.

These tests are pure (no SparkSession required) and run in milliseconds.
They verify that the generated DDL preserves reserved column names
via backtick-quoting, which is the regression that caused
DELTA_MERGE_UNRESOLVED_EXPRESSION in the original bug.
"""

import re

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from wearpipe.ddl import ddl_for_spec
from wearpipe.specs import MergeSpec, SilverSpec

# ============================================================
# Test schemas
# ============================================================

HEART_RATE_SCHEMA = StructType(
    [
        StructField('device_id', LongType(), True),
        StructField('time', TimestampType(), True),
        StructField('heartrate', DoubleType(), True),
        StructField('valid', BooleanType(), True),
    ]
)

USER_PROFILE_SCHEMA = StructType(
    [
        StructField('user_id', LongType(), True),
        StructField('update_type', StringType(), True),
        StructField('updated', TimestampType(), True),
        StructField('dob', DateType(), True),
        StructField('sex', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('state', StringType(), True),
    ]
)


# ============================================================
# Fixtures
# ============================================================

CATALOGUE = 'dev'
DB = 'db'


@pytest.fixture
def heart_rate_spec():
    """Minimal SilverSpec for heart_rate with the 4-column schema."""
    return SilverSpec(
        name='heart_rate',
        source='kafka_multiplex_bz',
        target_schema=HEART_RATE_SCHEMA,
        merge=MergeSpec(join_on='t.device_id = s.device_id AND t.time = s.time'),
    )


@pytest.fixture
def user_profile_spec():
    """Minimal SilverSpec for user_profile with reserved keyword columns."""
    return SilverSpec(
        name='user_profile',
        source='kafka_multiplex_bz',
        target_schema=USER_PROFILE_SCHEMA,
        merge=MergeSpec(join_on='t.user_id = s.user_id'),
    )


# ============================================================
# Helper
# ============================================================


def _backtick_quoted_names(ddl: str) -> list[str]:
    """Extract all backtick-quoted identifiers from a DDL string."""
    return re.findall(r'`([^`]+)`', ddl)


# ============================================================
# Tests
# ============================================================


class TestDdlStructure:
    """Verify the overall DDL string structure."""

    def test_starts_with_create_if_not_exists(self, heart_rate_spec):
        ddl = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        assert ddl.startswith('CREATE TABLE IF NOT EXISTS')

    def test_ends_with_using_delta(self, heart_rate_spec):
        ddl = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        assert ddl.endswith('USING DELTA')

    def test_table_name_is_fully_qualified(self, heart_rate_spec):
        ddl = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        assert 'dev.db.heart_rate' in ddl

    def test_ddl_is_deterministic(self, heart_rate_spec):
        """Same spec + same args must always produce identical DDL."""
        ddl1 = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        ddl2 = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        assert ddl1 == ddl2


class TestBacktickQuoting:
    """Verify that every column name is backtick-quoted."""

    def test_all_columns_are_backtick_quoted(self, heart_rate_spec):
        ddl = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        for field in HEART_RATE_SCHEMA.fields:
            assert f'`{field.name}`' in ddl, f'Column {field.name!r} is not backtick-quoted in DDL'

    def test_column_count_matches_schema(self, heart_rate_spec):
        """Number of backtick-quoted identifiers must equal number of fields."""
        ddl = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        quoted_names = _backtick_quoted_names(ddl)
        assert len(quoted_names) == len(HEART_RATE_SCHEMA.fields)

    def test_column_types_are_rendered(self, heart_rate_spec):
        """Each column must have its type rendered after the backtick-quoted name."""
        ddl = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        expected_type_pairs = [
            ('device_id', 'bigint'),
            ('time', 'timestamp'),
            ('heartrate', 'double'),
            ('valid', 'boolean'),
        ]
        for col_name, col_type in expected_type_pairs:
            assert f'`{col_name}` {col_type}' in ddl, f'Expected `{col_name}` {col_type} in DDL, got: {ddl}'


class TestReservedKeywordsPreserved:
    """Regression tests for the original bug where reserved identifiers
    were silently dropped or renamed by the Databricks SQL parser.

    These tests are the contract that prevents the bug from coming back
    if someone refactors ddl_for_spec or removes backticks.
    """

    def test_time_keyword_is_preserved(self, heart_rate_spec):
        """`time` is a reserved keyword and was dropped by CTAS without backticks."""
        ddl = ddl_for_spec(heart_rate_spec, CATALOGUE, DB)
        assert '`time`' in ddl

    def test_update_type_keyword_is_preserved(self, user_profile_spec):
        """`update_type` contains the reserved DML keyword `update`."""
        ddl = ddl_for_spec(user_profile_spec, CATALOGUE, DB)
        assert '`update_type`' in ddl

    def test_sex_keyword_is_preserved(self, user_profile_spec):
        """`sex` was observed to be dropped alongside `update_type`."""
        ddl = ddl_for_spec(user_profile_spec, CATALOGUE, DB)
        assert '`sex`' in ddl

    def test_state_keyword_is_preserved(self, user_profile_spec):
        """`state` is reserved in some SQL dialects."""
        ddl = ddl_for_spec(user_profile_spec, CATALOGUE, DB)
        assert '`state`' in ddl

    def test_all_reserved_columns_present_in_user_profile(self, user_profile_spec):
        """All 7 columns of user_profile must appear in the generated DDL."""
        ddl = ddl_for_spec(user_profile_spec, CATALOGUE, DB)
        quoted_names = set(_backtick_quoted_names(ddl))
        expected_names = {f.name for f in USER_PROFILE_SCHEMA.fields}
        assert quoted_names == expected_names


class TestEdgeCases:
    """Edge cases that protect against future regressions."""

    def test_empty_schema_produces_empty_column_list(self):
        """An empty StructType should produce DDL with empty parentheses."""
        empty_spec = SilverSpec(
            name='empty_table',
            source='dummy_source',
            target_schema=StructType([]),
            merge=MergeSpec(join_on='1=1'),
        )
        ddl = ddl_for_spec(empty_spec, CATALOGUE, DB)
        assert '()' in ddl
        assert 'CREATE TABLE IF NOT EXISTS dev.db.empty_table' in ddl

    def test_different_catalogue_and_db(self, heart_rate_spec):
        """The DDL must use the provided catalogue and db, not hardcode them."""
        ddl = ddl_for_spec(heart_rate_spec, 'prod', 'analytics')
        assert 'prod.analytics.heart_rate' in ddl
        assert 'dev.db' not in ddl

    def test_single_column_schema(self):
        """A single-column schema must produce valid DDL without trailing comma."""
        single_col_spec = SilverSpec(
            name='lookup',
            source='dummy',
            target_schema=StructType([StructField('id', LongType(), True)]),
            merge=MergeSpec(join_on='t.id = s.id'),
        )
        ddl = ddl_for_spec(single_col_spec, CATALOGUE, DB)
        assert '`id` bigint' in ddl
        # No trailing comma after the single column
        assert 'bigint,)' not in ddl
