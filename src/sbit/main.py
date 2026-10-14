import sys

from databricks.connect import DatabricksSession

from sbit.setup import SetupHelper


def main():
    try:
        spark = DatabricksSession.builder.serverless().getOrCreate()

        setup_manager = SetupHelper(spark)
        setup_manager.setup_db()
        setup_manager.validate_db()
        setup_manager.create_volume()
        setup_manager.validate_volume()
        setup_manager.cleanup()

    except Exception as e:
        print(f'Error: {e}')
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
