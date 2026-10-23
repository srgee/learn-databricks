from collections.abc import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def add_audit_columns(df: DataFrame) -> DataFrame:
    return df.withColumn('load_time', F.current_timestamp()).withColumn('source_file', F.col('_metadata.file_path'))


def join_date_lookup(df: DataFrame) -> DataFrame:
    lookup = df.sparkSession.table('date_lookup').select('date', 'week_part')
    cond = F.to_date((F.col('timestamp') / 1000).cast('timestamp')) == F.col('date')
    return df.join(F.broadcast(lookup), cond, 'left')


def parse_kafka_topic(topic: str, schema: str) -> Callable:
    """Return a transformation function to filter and parse a specific Kafka topic.

    Args:
        topic: The Kafka topic name to filter by.
        schema: The Spark schema to parse the JSON value column.

    Returns:
        A transformation function that filters by topic, parses the JSON payload,
        and flattens the resulting struct.
    """

    def _fn(df: DataFrame) -> DataFrame:
        return (
            df.filter(f'topic = "{topic}"')
            .select(F.from_json(F.col('value').cast('string'), schema).alias('v'))
            .select('v.*')
        )

    return _fn


def calculate_age_bins(dob_col):
    """Categorize date of birth into standardized age bins.

    Args:
        dob_col: The PySpark column containing date of birth values.

    Returns:
        A PySpark Column with the corresponding age bracket string.
    """
    age = F.floor(F.months_between(F.current_date(), dob_col) / 12)
    return (
        F.when(age < 18, 'under 18')
        .when(age < 25, '18-24')
        .when(age < 35, '25-34')
        .when(age < 45, '35-44')
        .when(age < 55, '45-54')
        .when(age < 65, '55-64')
        .when(age < 75, '65-74')
        .when(age < 85, '75-84')
        .when(age < 95, '85-94')
        .when(age >= 95, '95+')
        .otherwise('invalid_age')
        .alias('age')
    )
