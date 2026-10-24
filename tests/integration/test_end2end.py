from _helpers import DataProducer
from pyspark.sql import SparkSession

from sbit.bronze import BronzeIngester
from sbit.bronze_specs import FEEDS
from sbit.config import Config
from sbit.gold_specs import GOLD_PHASES, GOLD_STREAMS
from sbit.history_loader import load_history
from sbit.setup import SetupHelper
from sbit.silver_specs import SILVER_PHASES, SILVER_STREAMS
from sbit.streaming import UpsertPipeline


def test_batch(
    spark: SparkSession,
    config: Config,
    data_producer: DataProducer,
    date_lookup_file: str,
    data_files: list[str],
):
    """Two-iteration e2e: produce set 1 -> run -> validate BZ/SL/GL;
    produce set 2 -> run -> validate cumulative counts.

    Iteration 2 expects cumulative counts because bronze, silver,
    and gold Delta tables accumulate across iterations (no truncation
    between them — see SetupHelper.cleanup() that runs only at start
    and end of the module).
    """

    # 1. Setup Database
    sh = SetupHelper(spark, config.catalogue_name, config.db_name, config.volume_name)
    sh.cleanup_db()
    sh.setup_db()
    sh.setup_volume()

    try:
        # 2. History Load
        load_history(spark, date_lookup_file)

        # 3. Iteration 1: run all pipelines: Bronze, Silver and Gold
        data_producer.produce(1)

        # ----- BRONZE -----
        b_pipeline = BronzeIngester(spark, config.base_data_path, config.base_checkpoint_path)
        b_pipeline.ingest_all(FEEDS, once=True)

        table_fqn = f'{config.catalogue_name}.{config.db_name}.registered_users_bz'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 5

        table_fqn = f'{config.catalogue_name}.{config.db_name}.gym_logins_bz'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 8

        table_fqn = f'{config.catalogue_name}.{config.db_name}.kafka_multiplex_bz'
        actual_count = spark.read.table(table_fqn).where(filter="topic='user_info'").count()
        assert actual_count == 7

        table_fqn = f'{config.catalogue_name}.{config.db_name}.kafka_multiplex_bz'
        actual_count = spark.read.table(table_fqn).where(filter="topic='workout'").count()
        assert actual_count == 16

        table_fqn = f'{config.catalogue_name}.{config.db_name}.kafka_multiplex_bz'
        actual_count = spark.read.table(table_fqn).where(filter="topic='bpm'").count()
        assert actual_count == 253801

        # ----- SILVER -----
        s_pipeline = UpsertPipeline(spark, config.base_checkpoint_path)
        s_pipeline.run(SILVER_STREAMS, SILVER_PHASES, once=True)

        table_fqn = f'{config.catalogue_name}.{config.db_name}.users'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 5

        table_fqn = f'{config.catalogue_name}.{config.db_name}.gym_logs'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 8

        table_fqn = f'{config.catalogue_name}.{config.db_name}.user_profile'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 5

        table_fqn = f'{config.catalogue_name}.{config.db_name}.workouts'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 15

        table_fqn = f'{config.catalogue_name}.{config.db_name}.heart_rate'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 253801

        table_fqn = f'{config.catalogue_name}.{config.db_name}.user_bins'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 5

        table_fqn = f'{config.catalogue_name}.{config.db_name}.completed_workouts'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 8

        table_fqn = f'{config.catalogue_name}.{config.db_name}.workout_bpm'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 3968

        # ----- GOLD -----
        g_pipeline = UpsertPipeline(spark, config.base_checkpoint_path)
        g_pipeline.run(GOLD_STREAMS, GOLD_PHASES, once=True)

        table_fqn = f'{config.catalogue_name}.{config.db_name}.gym_summary'
        actual_rows = spark.table(table_fqn).collect()
        assert actual_rows == 8

        table_fqn = f'{config.catalogue_name}.{config.db_name}.workout_bpm_summary'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 2

        # 3. Iteration 2
        data_producer.produce(2)

        # ----- BRONZE -----
        b_pipeline = BronzeIngester(spark, config.base_data_path, config.base_checkpoint_path)
        b_pipeline.ingest_all(FEEDS, once=True)

        table_fqn = f'{config.catalogue_name}.{config.db_name}.registered_users_bz'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 10

        table_fqn = f'{config.catalogue_name}.{config.db_name}.gym_logins_bz'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 16

        table_fqn = f'{config.catalogue_name}.{config.db_name}.kafka_multiplex_bz'
        actual_count = spark.read.table(table_fqn).where(filter="topic='user_info'").count()
        assert actual_count == 13

        table_fqn = f'{config.catalogue_name}.{config.db_name}.kafka_multiplex_bz'
        actual_count = spark.read.table(table_fqn).where(filter="topic='workout'").count()
        assert actual_count == 32

        table_fqn = f'{config.catalogue_name}.{config.db_name}.kafka_multiplex_bz'
        actual_count = spark.read.table(table_fqn).where(filter="topic='bpm'").count()
        assert actual_count == 253801 * 2

        # ----- SILVER -----
        s_pipeline = UpsertPipeline(spark, config.base_checkpoint_path)
        s_pipeline.run(SILVER_STREAMS, SILVER_PHASES, once=True)

        table_fqn = f'{config.catalogue_name}.{config.db_name}.users'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 10

        table_fqn = f'{config.catalogue_name}.{config.db_name}.gym_logs'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 16

        table_fqn = f'{config.catalogue_name}.{config.db_name}.user_profile'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 10

        table_fqn = f'{config.catalogue_name}.{config.db_name}.workouts'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 32

        table_fqn = f'{config.catalogue_name}.{config.db_name}.heart_rate'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 253801 * 2

        table_fqn = f'{config.catalogue_name}.{config.db_name}.user_bins'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 10

        table_fqn = f'{config.catalogue_name}.{config.db_name}.completed_workouts'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 16

        table_fqn = f'{config.catalogue_name}.{config.db_name}.workout_bpm'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 8192

        # ----- GOLD -----
        g_pipeline = UpsertPipeline(spark, config.base_checkpoint_path)
        g_pipeline.run(GOLD_STREAMS, GOLD_PHASES, once=True)

        table_fqn = f'{config.catalogue_name}.{config.db_name}.gym_summary'
        actual_rows = spark.table(table_fqn).collect()
        assert actual_rows == 16

        table_fqn = f'{config.catalogue_name}.{config.db_name}.workout_bpm_summary'
        actual_count = spark.read.table(table_fqn).count()
        assert actual_count == 2

    finally:
        sh.cleanup_volume()
        sh.cleanup_db()


def test_streaming():
    pass
