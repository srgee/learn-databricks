from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from wearpipe.specs import GoldSpec, MergeSpec


def build_workout_bpm_summary(spark: SparkSession, starting_version: int):
    """Streaming aggregation with watermark + static dimension join."""
    df_bpm = (
        spark.readStream.option('startingVersion', starting_version)
        .table('workout_bpm')
        .withWatermark('end_time', '30 seconds')
        .groupBy('user_id', 'workout_id', 'session_id', 'end_time')
        .agg(
            F.min('heartrate').alias('min_bpm'),
            F.mean('heartrate').alias('avg_bpm'),
            F.max('heartrate').alias('max_bpm'),
            F.count('heartrate').alias('num_recordings'),
        )
    )

    df_users = spark.read.table('user_bins')
    return df_bpm.join(df_users, ['user_id']).select(
        'workout_id',
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
    )


GOLD_STREAMS: dict[str, GoldSpec] = {
    'workout_bpm_summary': GoldSpec(
        name='workout_bpm_summary',
        source='workout_bpm',
        build_df=build_workout_bpm_summary,  # Not following the pattern aggregation + static join
        target_schema=StructType(
            [
                StructField('workout_id', IntegerType(), True),
                StructField('session_id', IntegerType(), True),
                StructField('user_id', IntegerType(), True),
                StructField('age', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('city', StringType(), True),
                StructField('state', StringType(), True),
                StructField('min_bpm', DoubleType(), True),
                StructField('avg_bpm', DoubleType(), True),
                StructField('max_bpm', DoubleType(), True),
                StructField('num_recordings', LongType(), True),
            ]
        ),
        # Idempotent: completed session does not change → MERGE insert-only
        merge=MergeSpec(
            join_on='t.user_id = s.user_id AND t.workout_id = s.workout_id AND t.session_id = s.session_id'
        ),
    ),
}

GOLD_PHASES = [list(GOLD_STREAMS)]
