import sys
import time

from databricks.connect.session import DatabricksSession

from sbit.bronze import BronzeIngester
from sbit.bronze_specs import FEEDS
from sbit.config import load_settings
from sbit.gold_specs import GOLD_PHASES, GOLD_STREAMS
from sbit.io import build_object_path
from sbit.silver_specs import PHASES, SILVER_STREAMS
from sbit.streaming import UpsertPipeline


def main():
    try:
        config = load_settings()

        spark = DatabricksSession.builder.getOrCreate()
        spark.sql(f'USE `{config.catalogue_name}`.`{config.db_name}`')

        start_time = int(time.time())
        print(f'\nStarting {config.layer.upper()} pipeline(s) ...')
        once = not config.stream_flag
        if config.layer == 'bronze':
            # --- INGEST RAW DATA (BRONZE PIPELINE) ---
            base_dir = build_object_path(
                config.catalogue_name, config.db_name, config.volume_name, config.base_data_path, '/raw'
            )
            BronzeIngester(spark, base_dir, config.base_checkpoint_path).ingest_all(FEEDS, once=once)

        elif config.layer == 'silver':
            # --- RUN SILVER PIPELINE ---
            checkpoint_root = build_object_path(
                config.catalogue_name, config.db_name, config.volume_name, config.base_data_path, '/silver'
            )
            UpsertPipeline(spark, checkpoint_root, starting_version=config.starting_version).run(
                SILVER_STREAMS, PHASES, once=once
            )
        elif config.layer == 'gold':
            # --- RUN GOLD PIPELINE ---
            checkpoint_root = build_object_path(
                config.catalogue_name, config.db_name, config.volume_name, config.base_data_path, '/gold'
            )
            UpsertPipeline(spark, checkpoint_root, starting_version=config.starting_version).run(
                GOLD_STREAMS, GOLD_PHASES, once=once
            )
        elif config.layer == 'all':
            print('Starting to run BRONZE pipeline ... ', end='')
            base_dir = build_object_path(
                config.catalogue_name, config.db_name, config.volume_name, config.base_data_path, '/raw'
            )
            BronzeIngester(spark, base_dir, config.base_checkpoint_path).ingest_all(FEEDS, once=once)
            print('Done!')
            print('Starting to run SILVER pipeline ... ', end='')
            checkpoint_root = build_object_path(
                config.catalogue_name, config.db_name, config.volume_name, config.base_data_path, '/silver'
            )
            UpsertPipeline(spark, checkpoint_root, starting_version=config.starting_version).run(
                SILVER_STREAMS, PHASES, once=once
            )
            print('Done!')
            print('Starting to run GOLD pipeline ... ', end='')
            checkpoint_root = build_object_path(
                config.catalogue_name, config.db_name, config.volume_name, config.base_data_path, '/gold'
            )
            UpsertPipeline(spark, checkpoint_root, starting_version=config.starting_version).run(
                GOLD_STREAMS, GOLD_PHASES, once=once
            )
            print('Done!')

        print(f'{config.layer.upper()} pipeline(s) completed in {int(time.time()) - start_time} seconds')

    except Exception as e:
        print(f'Error: {e}')
        sys.exit(1)
    finally:
        if 'spark' in locals() and spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
