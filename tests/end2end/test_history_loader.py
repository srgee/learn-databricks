from pyspark.sql import SparkSession

from sbit.config import Config
from sbit.history_loader import _transform_date_lookup, load_history


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
    # Ruta construida a partir de la configuración o la ruta preexistente en Databricks
    file_path = f'/Volumes/{config.catalogue_name}/{config.db_name}/{config.volume_name}/{config.base_data_path}/date-lookup.json'  # noqa: E501

    # Ejecutar carga histórica directamente
    load_history(spark, file_path)

    # Validaciones en destino
    table_name = 'date_lookup'
    assert spark.catalog.tableExists(table_name)
    assert spark.read.table(table_name).count() > 0
