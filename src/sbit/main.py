from databricks.connect import DatabricksSession

from sbit.setup import SetupHelper


def main():
    spark = DatabricksSession.builder.serverless().getOrCreate()

    db_manager = SetupHelper('sbit', spark)
    db_manager.setup()
    db_manager.validate()
    db_manager.cleanup()

    spark.stop()


if __name__ == '__main__':
    main()
