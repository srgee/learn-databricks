import sys

from databricks.connect.session import DatabricksSession

from sbit.history_loader import load_history, validate_history_data


def main():
    try:
        spark = DatabricksSession.builder.serverless().getOrCreate()

        """setup_manager = SetupHelper(spark)
        setup_manager.setup_db()
        setup_manager.validate_db()
        setup_manager.validate_volume()
        setup_manager.cleanup()"""

        load_history(spark)
        validate_history_data(spark)

        """
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(profile='mi-databricks')
        for f in w.dbutils.fs.ls('/Volumes/dev/sbit_db/landing_zone/data_zone/test_data'):
            print(f.path)

        spark.read.option('multiline', 'true').format('json').load(
            '/Volumes/dev/sbit_db/landing_zone/data_zone/test_data/date-lookup.json'
        ).show(5)"""

    except Exception as e:
        print(f'Error: {e}')
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
