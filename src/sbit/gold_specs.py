from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sbit.streaming import StreamSpec
from sbit.upserters import MergeSpec


def build_workout_bpm_summary(spark: SparkSession, starting_version: int):
    """Streaming aggregation with watermark + static dimension join."""
    df_bpm = (
        spark.readStream.option('startingVersion', starting_version)
        .table('workout_bpm')
        .withWatermark('end_time', '30 seconds')
        .groupBy('user_id', 'workout_id', 'session_id', 'end_time')
        .agg(
            F.min('heart_rate').alias('min_bpm'),
            F.mean('heart_rate').alias('avg_bpm'),
            F.max('heart_rate').alias('max_bpm'),
            F.count('heart_rate').alias('num_recordings'),
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


GOLD_STREAMS: dict[str, StreamSpec] = {
    'workout_bpm_summary': StreamSpec(
        name='workout_bpm_summary',
        source='workout_bpm',
        build_df=build_workout_bpm_summary,  # not following the pattern aggregation + static join
        # Idempotent: completed session doe snot change → MERGE insert-only
        merge=MergeSpec(
            join_on='t.user_id = s.user_id AND t.workout_id = s.workout_id AND t.session_id = s.session_id'
        ),
        output_mode='append',
    ),
}

GOLD_PHASES = [list(GOLD_STREAMS)]
