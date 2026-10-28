import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from wearpipe.gold import _verify_schema_match

# ============================================================
# Test fixture: workout_bpm_summary schema
# ============================================================

WORKOUT_BPM_SUMMARY_SCHEMA = StructType(
    [
        StructField('workout_id', IntegerType(), True),
        StructField('session_id', IntegerType(), True),
        StructField('user_id', LongType(), True),
        StructField('age', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('min_bpm', DoubleType(), True),
        StructField('avg_bpm', DoubleType(), True),
        StructField('max_bpm', DoubleType(), True),
        StructField('num_recordings', LongType(), True),
    ]
)


# ============================================================
# Tests
# ============================================================


class TestSchemaMatch:
    def test_identical_schema_passes(self):
        _verify_schema_match(
            'workout_bpm_summary',
            WORKOUT_BPM_SUMMARY_SCHEMA,
            WORKOUT_BPM_SUMMARY_SCHEMA,
        )

    def test_different_order_passes(self):
        reordered = StructType(list(reversed(WORKOUT_BPM_SUMMARY_SCHEMA.fields)))
        _verify_schema_match(
            'workout_bpm_summary',
            WORKOUT_BPM_SUMMARY_SCHEMA,
            reordered,
        )


class TestMissingColumns:
    def test_missing_state_raises(self):
        """`state` is reserved in some SQL dialects — must be detected if dropped."""
        actual = StructType(
            [
                StructField('workout_id', IntegerType(), True),
                StructField('session_id', IntegerType(), True),
                StructField('user_id', LongType(), True),
                StructField('age', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('city', StringType(), True),
                # state MISSING
                StructField('min_bpm', DoubleType(), True),
                StructField('avg_bpm', DoubleType(), True),
                StructField('max_bpm', DoubleType(), True),
                StructField('num_recordings', LongType(), True),
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('workout_bpm_summary', WORKOUT_BPM_SUMMARY_SCHEMA, actual)
        msg = str(exc_info.value)
        assert '[workout_bpm_summary]' in msg
        assert 'missing in stage' in msg
        assert 'state' in msg

    def test_missing_multiple_columns_all_listed(self):
        actual = StructType(
            [
                StructField('workout_id', IntegerType(), True),
                # everything else MISSING
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('workout_bpm_summary', WORKOUT_BPM_SUMMARY_SCHEMA, actual)
        msg = str(exc_info.value)
        for col in [
            'session_id',
            'user_id',
            'age',
            'gender',
            'city',
            'state',
            'min_bpm',
            'avg_bpm',
            'max_bpm',
            'num_recordings',
        ]:
            assert col in msg


class TestExtraColumns:
    def test_extra_column_raises(self):
        actual = StructType(
            [
                StructField('workout_id', IntegerType(), True),
                StructField('session_id', IntegerType(), True),
                StructField('user_id', LongType(), True),
                StructField('age', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('city', StringType(), True),
                StructField('state', StringType(), True),
                StructField('min_bpm', DoubleType(), True),
                StructField('avg_bpm', DoubleType(), True),
                StructField('max_bpm', DoubleType(), True),
                StructField('num_recordings', LongType(), True),
                StructField('unexpected_col', StringType(), True),  # extra
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('workout_bpm_summary', WORKOUT_BPM_SUMMARY_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'extra in stage' in msg
        assert 'unexpected_col' in msg


class TestTypeMismatches:
    def test_user_id_type_mismatch_raises(self):
        """user_id declared as LongType but stage produces IntegerType."""
        actual = StructType(
            [
                StructField('workout_id', IntegerType(), True),
                StructField('session_id', IntegerType(), True),
                StructField('user_id', IntegerType(), True),  # was LongType
                StructField('age', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('city', StringType(), True),
                StructField('state', StringType(), True),
                StructField('min_bpm', DoubleType(), True),
                StructField('avg_bpm', DoubleType(), True),
                StructField('max_bpm', DoubleType(), True),
                StructField('num_recordings', LongType(), True),
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('workout_bpm_summary', WORKOUT_BPM_SUMMARY_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'type mismatches' in msg
        assert 'user_id' in msg
        assert 'bigint' in msg  # expected
        assert 'int' in msg  # actual

    def test_num_recordings_type_mismatch_raises(self):
        """num_recordings declared as LongType but stage produces IntegerType."""
        actual = StructType(
            [
                StructField('workout_id', IntegerType(), True),
                StructField('session_id', IntegerType(), True),
                StructField('user_id', LongType(), True),
                StructField('age', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('city', StringType(), True),
                StructField('state', StringType(), True),
                StructField('min_bpm', DoubleType(), True),
                StructField('avg_bpm', DoubleType(), True),
                StructField('max_bpm', DoubleType(), True),
                StructField('num_recordings', IntegerType(), True),  # was LongType
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('workout_bpm_summary', WORKOUT_BPM_SUMMARY_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'num_recordings' in msg
        assert 'bigint' in msg  # expected
        assert 'int' in msg  # actual


class TestCombinedMismatches:
    def test_missing_extra_and_type_mismatch_all_reported(self):
        actual = StructType(
            [
                StructField('workout_id', IntegerType(), True),
                StructField('session_id', IntegerType(), True),
                StructField('user_id', IntegerType(), True),  # type mismatch
                # age, gender MISSING
                StructField('city', StringType(), True),
                StructField('state', StringType(), True),
                StructField('min_bpm', DoubleType(), True),
                StructField('avg_bpm', DoubleType(), True),
                StructField('max_bpm', DoubleType(), True),
                StructField('num_recordings', LongType(), True),
                StructField('extra_col', StringType(), True),  # extra
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('workout_bpm_summary', WORKOUT_BPM_SUMMARY_SCHEMA, actual)
        msg = str(exc_info.value)
        assert 'missing in stage' in msg
        assert 'age' in msg
        assert 'gender' in msg
        assert 'extra in stage' in msg
        assert 'extra_col' in msg
        assert 'type mismatches' in msg
        assert 'user_id' in msg


class TestErrorMessage:
    def test_error_includes_spec_name(self):
        actual = StructType([])
        with pytest.raises(RuntimeError) as exc_info:
            _verify_schema_match('my_gold_spec', WORKOUT_BPM_SUMMARY_SCHEMA, actual)
        assert '[my_gold_spec]' in str(exc_info.value)

    def test_error_is_runtime_error(self):
        with pytest.raises(RuntimeError):
            _verify_schema_match('x', WORKOUT_BPM_SUMMARY_SCHEMA, StructType([]))
