import sys

from databricks.connect.session import DatabricksSession

from sbit.config import load_settings
from sbit.history_loader import load_history, validate_history_data
from sbit.io import build_object_path


def main():
    try:
        config = load_settings()

        spark = DatabricksSession.builder.getOrCreate()
        spark.sql(f'USE `{config.catalogue_name}`.`{config.db_name}`')

        """setup_manager = SetupHelper(spark)
        setup_manager.setup_db()
        setup_manager.validate_db()
        setup_manager.validate_volume()
        setup_manager.cleanup()"""

        # LOAD HISTORY DATA
        file_path = build_object_path(
            config.catalogue_name,
            config.db_name,
            config.volume_name,
            config.base_data_path,
            'test_data',
            'date-lookup.json',
        )
        load_history(spark, file_path)
        validate_history_data(spark)

    except Exception as e:
        print(f'Error: {e}')
        sys.exit(1)
    finally:
        if 'spark' in locals() and spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
