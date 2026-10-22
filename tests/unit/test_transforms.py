from datetime import date, datetime

import pyspark.sql.functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from sbit.transforms import (
    add_audit_columns,
    calculate_age_bins,
    join_date_lookup,
    parse_kafka_topic,
)


def test_add_audit_columns(spark, date_lookup_file):
    df = spark.read.json(date_lookup_file)
    result = add_audit_columns(df)

    assert 'load_time' in result.columns
    assert 'source_file' in result.columns
    assert result.collect()[0]['source_file'] is not None


def test_join_date_lookup(spark):
    # Registrar tabla temporal para simular date_lookup
    lookup_data = [(date(2026, 1, 1), 'workday')]
    spark.createDataFrame(lookup_data, ['date', 'week_part']).createOrReplaceTempView('date_lookup')

    # Timestamp para 2026-01-01 00:00:00 UTC en milisegundos (1767225600000)
    df = spark.createDataFrame([(1767225600000,)], ['timestamp'])

    result = join_date_lookup(df)

    assert 'week_part' in result.columns
    assert result.collect()[0]['week_part'] == 'workday'


def test_parse_kafka_topic(spark):
    schema = StructType([StructField('id', LongType(), True), StructField('msg', StringType(), True)])
    data = [('topic_a', '{"id": 1, "msg": "hello"}'), ('topic_b', '{"id": 2, "msg": "world"}')]
    df = spark.createDataFrame(data, ['topic', 'value'])

    transform = parse_kafka_topic('topic_a', schema)
    result = transform(df)

    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]['id'] == 1
    assert rows[0]['msg'] == 'hello'


def test_calculate_age_bins(spark):
    current_year = datetime.now().year
    dob_str = f'{current_year - 30}-06-01'  # 30 años -> bin 25-34

    df = spark.createDataFrame([(dob_str,)], ['dob']).withColumn('dob', F.to_date('dob'))
    result = df.select(calculate_age_bins(F.col('dob')))

    assert result.collect()[0]['age'] == '25-34'
