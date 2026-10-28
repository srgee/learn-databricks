"""Unit tests for wearpipe.silver._verify_schema_match.

These tests are pure (no SparkSession required) and validate that the
schema check correctly detects:
  - Missing columns in the actual schema.
  - Extra columns in the actual schema.
  - Type mismatches between expected and actual schemas.
  - All three issues reported together in a single error message.

The check is the contract that prevents silent schema drift between
SilverSpec.target_schema and the actual transform output, which was
the root cause of the original DELTA_MERGE_UNRESOLVED_EXPRESSION bug.
"""

import pytest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from wearpipe.silver import _verify_schema_match

# ============================================================
# Test fixtures: realistic schemas matching the actual silver specs
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
        StructField('dob', StringType(), True),  # note: spec casts to date, but transform can vary
        StructField('sex', StringType(), True),
        StructField('gender', StringType(), True),
    ]
)


# ============================================================
# Tests
# ============================================================


class TestSchemaMatch:
    """Happy path: schemas match in all valid configurations."""

    def test_identical_schema_passes(self):
        """Same fields, same types, same order → no exception."""
        _verify_schema_match('heart_rate', HEART_RATE_SCHEMA, HEART_RATE_SCHEMA)

    def test_different_order_passes(self):
        """Delta does not guarantee column order; the check must be order-agnostic."""
        reordered = StructType(
            [
                HEART_RATE_SCHEMA.fields[3],  # valid
                HEART_RATE_SCHEMA.fields[2],  # heartrate
                HEART_RATE_SCHEMA.fields[1],  # time
                HEART_RATE_SCHEMA.fields[0],  # device_id
            ]
        )
        _verify_schema_match('heart_rate', HEART_RATE_SCHEMA, reordered)

    def test_different_nullability_passes(self):
        """Nullability is not strictly enforced by the merge builder."""
        different_nullable = StructType(
            [
                StructField('device_id', LongType(), False),  # was True
                StructField('time', TimestampType(), True),
                StructField('heartrate', DoubleType(), True),
                StructField('valid', BooleanType(), True),
            ]
        )
        _verify_schema_match('heart_rate', HEART_RATE_SCHEMA, different_nullable)


class TestMissingColumns:
    """Regressions for the original bug: reserved keywords dropped from stage."""

    def test_missing_time_raises_with_clear_message(self):
        """`time` was silently dropped by CTAS — must be detected here."""
        actual = StructType(
            [
                StructField('device_id', LongType(), True),
                # time MISSING — this is the original bug
                StructField('heartrate', DoubleType(), True),
                StructField('valid', BooleanType(), True),
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('heart_rate', HEART_RATE_SCHEMA, actual)
        msg = str(exc_info.value)
        assert '[heart_rate]' in msg
        assert 'missing in stage' in msg
        assert 'time' in msg

    def test_missing_multiple_columns_raises_with_all_listed(self):
        """All missing columns must appear in the error message."""
        actual = StructType(
            [
                StructField('user_id', LongType(), True),
                # update_type and sex MISSING — both were dropped in the original bug
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('user_profile', USER_PROFILE_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'update_type' in msg
        assert 'sex' in msg


class TestExtraColumns:
    """Stage with undeclared columns must also fail."""

    def test_extra_column_raises_with_clear_message(self):
        actual = StructType(
            [
                StructField('device_id', LongType(), True),
                StructField('time', TimestampType(), True),
                StructField('heartrate', DoubleType(), True),
                StructField('valid', BooleanType(), True),
                StructField('unexpected_col', StringType(), True),  # extra
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('heart_rate', HEART_RATE_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'extra in stage' in msg
        assert 'unexpected_col' in msg


class TestTypeMismatches:
    """Same column names but different types must be detected."""

    def test_single_type_mismatch_raises(self):
        """device_id declared as LongType but stage produces IntegerType."""
        actual = StructType(
            [
                StructField('device_id', IntegerType(), True),  # was LongType
                StructField('time', TimestampType(), True),
                StructField('heartrate', DoubleType(), True),
                StructField('valid', BooleanType(), True),
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('heart_rate', HEART_RATE_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'type mismatches' in msg
        assert 'device_id' in msg
        assert 'bigint' in msg  # expected
        assert 'int' in msg  # actual

    def test_multiple_type_mismatches_all_listed(self):
        actual = StructType(
            [
                StructField('user_id', IntegerType(), True),  # was LongType
                StructField('update_type', StringType(), True),
                StructField('updated', StringType(), True),  # was TimestampType
                StructField('dob', StringType(), True),
                StructField('sex', StringType(), True),
                StructField('gender', StringType(), True),
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('user_profile', USER_PROFILE_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'user_id' in msg
        assert 'updated' in msg


class TestCombinedMismatches:
    """When multiple issues coexist, all must appear in the error message."""

    def test_missing_extra_and_type_mismatch_all_reported(self):
        actual = StructType(
            [
                StructField('device_id', IntegerType(), True),  # type mismatch (was LongType)
                # time MISSING
                StructField('heartrate', DoubleType(), True),
                StructField('valid', BooleanType(), True),
                StructField('extra_col', StringType(), True),  # extra
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('heart_rate', HEART_RATE_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'missing in stage' in msg
        assert 'time' in msg
        assert 'extra in stage' in msg
        assert 'extra_col' in msg
        assert 'type mismatches' in msg
        assert 'device_id' in msg


class TestErrorMessage:
    """Validate the structure of the error message for diagnosis."""

    def test_error_includes_spec_name(self):
        """Spec name must be in the message for cross-spec diagnosis."""
        actual = StructType([])  # totally empty — guaranteed mismatch
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('my_special_spec', HEART_RATE_SCHEMA, actual)
        assert '[my_special_spec]' in str(exc_info.value)

    def test_error_is_runtime_error_not_other_type(self):
        """Must be RuntimeError specifically (not AnalysisException, etc.)."""
        with pytest.raises(RuntimeError):
            _verify_schema_match('x', HEART_RATE_SCHEMA, StructType([]))
