from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sbit.streaming import StreamSpec
from sbit.transforms import calculate_age_bins, parse_kafka_topic
from sbit.upserters import MergeSpec, dedup_latest


def _get_cdc_options(starting_version: int, ignore_changes: bool = False) -> dict:
    key = 'ignoreChanges' if ignore_changes else 'ignoreDeletes'
    return {'startingVersion': starting_version, key: 'true'}


def build_completed_workouts(spark: SparkSession, starting_version: int) -> DataFrame:
    """Build a streaming DataFrame of completed workouts by joining start and stop events.

    Args:
        spark: The active SparkSession instance.
        starting_version: The starting version for the CDC stream reader.

    Returns:
        A streaming DataFrame containing matched start and stop workout
        sessions.
    """
    opts = _get_cdc_options(starting_version)
    df_start = (
        spark.readStream.options(**opts)
        .table('workouts')
        .filter("action = 'start'")
        .selectExpr('user_id', 'workout_id', 'session_id', 'time as start_time')
        .withWatermark('start_time', '30 seconds')
    )
    df_stop = (
        spark.readStream.options(**opts)
        .table('workouts')
        .filter("action = 'stop'")
        .selectExpr('user_id', 'workout_id', 'session_id', 'time as end_time')
        .withWatermark('end_time', '30 seconds')
    )
    cond = [
        df_start.user_id == df_stop.user_id,
        df_start.workout_id == df_stop.workout_id,
        df_start.session_id == df_stop.session_id,
        df_stop.end_time < df_start.start_time + F.expr('interval 3 hours'),  # state cleanup
    ]
    return df_start.join(df_stop, cond).select(
        df_start.user_id,
        df_start.workout_id,
        df_start.session_id,
        df_start.start_time,
        df_stop.end_time,
    )


def build_workout_bpm(spark: SparkSession, starting_version: int) -> DataFrame:
    """Build a streaming DataFrame enriching completed workouts with heart rate metrics.

    Args:
        spark: The active SparkSession instance.
        starting_version: The starting version for the CDC stream reader.

    Returns:
        A streaming DataFrame containing heart rate data points matched to specific
        workout sessions.
    """
    opts = _get_cdc_options(starting_version)
    df_cw = (
        spark.readStream.options(**opts)
        .table('completed_workouts')
        .join(spark.read.table('users'), 'user_id')
        .selectExpr('user_id', 'device_id', 'workout_id', 'session_id', 'start_time', 'end_time')
        .withWatermark('end_time', '30 seconds')
    )
    df_bpm = (
        spark.readStream.options(**opts)
        .table('heart_rate')
        .filter('valid = true')
        .selectExpr('device_id', 'time', 'heartrate')
        .withWatermark('time', '30 seconds')
    )
    cond = [
        df_cw.device_id == df_bpm.device_id,
        df_bpm.time > df_cw.start_time,
        df_bpm.time <= df_cw.end_time,
        df_cw.end_time < df_bpm.time + F.expr('interval 3 hours'),  # state cleanup
    ]
    return df_bpm.join(df_cw, cond).select(
        'user_id',
        'workout_id',
        'session_id',
        'start_time',
        'end_time',
        'time',
        'heartrate',
    )


# --- DECLARE TARGETS ---
SILVER_STREAMS = {
    'users': StreamSpec(
        name='users',
        source='registered_users_bz',
        transform=lambda df: df.selectExpr(
            'user_id', 'device_id', 'mac_address', 'cast(registration_timestamp as timestamp)'
        ),
        merge=MergeSpec(join_on='t.user_id = s.user_id'),  # insert-only: no puede re-registrarse
        dedupe_keys=('user_id', 'device_id'),
        watermark_col='registration_timestamp',
    ),
    'gym_logs': StreamSpec(
        name='gym_logs',
        source='gym_logins_bz',
        transform=lambda df: df.selectExpr(
            'mac_address', 'gym', 'cast(login as timestamp)', 'cast(logout as timestamp)'
        ),
        merge=MergeSpec(
            join_on='t.mac_address = s.mac_address AND t.gym = s.gym AND t.login = s.login',
            update=True,
            update_condition='s.logout > t.login AND s.logout > t.logout',
            update_set={'logout': 's.logout'},
        ),
        dedupe_keys=('mac_address', 'gym', 'login'),
        watermark_col='login',
    ),
    'user_profile': StreamSpec(
        name='user_profile',
        source='kafka_multiplex_bz',
        transform=lambda df: parse_kafka_topic(
            'user_info',
            """
            user_id bigint, update_type STRING, timestamp FLOAT,
            dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING,
            address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>""",
        )(df).select(
            'user_id',
            'update_type',
            F.col('timestamp').cast('timestamp').alias('updated'),
            F.to_date(F.col('dob'), 'MM/dd/yyyy').alias('dob'),
            'sex',
            'gender',
            'first_name',
            'last_name',
            F.col('address.street_address').alias('street_address'),
            F.col('address.city').alias('city'),
            F.col('address.state').alias('state'),
            F.col('address.zip').alias('zip'),
        ),
        batch_transform=dedup_latest('user_id', 'updated'),
        merge=MergeSpec(join_on='t.user_id = s.user_id', update=True, update_condition='t.updated < s.updated'),
        dedupe_keys=('user_id', 'updated'),
        watermark_col='updated',
    ),
    'workouts': StreamSpec(
        name='workouts',
        source='kafka_multiplex_bz',
        transform=lambda df: parse_kafka_topic(
            'workout', 'user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT'
        )(df).select(
            'user_id', 'workout_id', F.col('timestamp').cast('timestamp').alias('time'), 'action', 'session_id'
        ),
        merge=MergeSpec(join_on='t.user_id = s.user_id AND t.time = s.time'),
        dedupe_keys=('user_id', 'time'),
        watermark_col='time',
    ),
    'heart_rate': StreamSpec(
        name='heart_rate',
        source='kafka_multiplex_bz',
        transform=lambda df: parse_kafka_topic('bpm', 'device_id LONG, time TIMESTAMP, heartrate DOUBLE')(
            df
        ).withColumn('valid', F.when(F.col('heartrate') <= 0, False).otherwise(True)),
        merge=MergeSpec(join_on='t.device_id = s.device_id AND t.time = s.time'),
        dedupe_keys=('device_id', 'time'),
        watermark_col='time',
    ),
    'user_bins': StreamSpec(
        name='user_bins',
        source='user_profile',
        ignore_changes=True,
        transform=lambda df: df.join(
            df.sparkSession.table('users').select('user_id').distinct(), 'user_id', 'left'
        ).select('user_id', calculate_age_bins(F.col('dob')), 'gender', 'city', 'state'),
        merge=MergeSpec(join_on='t.user_id = s.user_id', update=True),
    ),
    'completed_workouts': StreamSpec(
        name='completed_workouts',
        source='workouts',
        build_df=build_completed_workouts,
        output_mode='append',
        merge=MergeSpec(
            join_on='t.user_id = s.user_id AND t.workout_id = s.workout_id AND t.session_id = s.session_id'
        ),
    ),
    'workout_bpm': StreamSpec(
        name='workout_bpm',
        source='completed_workouts',
        build_df=build_workout_bpm,
        output_mode='append',
        merge=MergeSpec(
            join_on='t.user_id = s.user_id AND t.workout_id = s.workout_id '
            'AND t.session_id = s.session_id AND t.time = s.time'
        ),
    ),
}

SILVER_PHASES: list[list[str]] = [
    ['users', 'gym_logs', 'user_profile', 'workouts', 'heart_rate'],
    ['user_bins', 'completed_workouts'],
    ['workout_bpm'],
]
