from pyspark.sql import SparkSession

from wearpipe.config import Config
from wearpipe.history_loader import _transform_date_lookup, load_history
from wearpipe.setup import SetupHelper


def test_transform_date_lookup(spark: SparkSession):
    data = [
        {
            'date': '2026-01-01',
            'week': '1',
            'year': '2026',
            'month': '1',
            'dayofweek': '4',
            'dayofmonth': '1',
            'dayofyear': '1',
            'week_part': 'workday',
        }
    ]
    df = spark.createDataFrame(data)
    transformed = _transform_date_lookup(df)

    assert transformed.schema['date'].dataType.simpleString() == 'date'
    assert transformed.schema['week'].dataType.simpleString() == 'int'


def test_end_to_end_load_history(spark: SparkSession, config: Config):
    try:
        setup = SetupHelper(spark, config.catalogue_name, config.db_name, config.volume_name, config.base_data_path)
        setup.setup_db()
        setup.setup_volume()
        setup.create_date_lookup_table()
        file_path = setup.upload_date_lookup_file()

        load_history(spark, file_path)

        table_name = 'date_lookup'
        assert spark.catalog.tableExists(table_name)
        assert spark.read.table(table_name).count() > 0

    finally:
        setup.cleanup_volume()
        setup.cleanup_db()
