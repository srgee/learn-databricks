import time
from typing import Any

# Schema contract for data upload
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Schema contract for data upload
DATE_LOOKUP_TYPES: dict[str, str] = {
    'date': 'date',
    'week': 'int',
    'year': 'int',
    'month': 'int',
    'dayofweek': 'int',
    'dayofmonth': 'int',
    'dayofyear': 'int',
    'week_part': 'string',
}


def _transform_date_lookup(df: DataFrame) -> DataFrame:
    """Pure function: apply schema contract. Testeable with no I/O."""
    return df.select([F.col(col).cast(dtype).alias(col) for col, dtype in DATE_LOOKUP_TYPES.items()])


def _load_date_lookup(spark: Any, path: str) -> None:
    (
        _transform_date_lookup(spark.read.option('multiline', 'true').format('json').load(path))
        .write.mode('overwrite')
        .saveAsTable('date_lookup')
    )


def load_history(spark: Any, path: str) -> None:
    start_time = int(time.time())
    print('\nStarting historical data load ...')
    _load_date_lookup(spark, path)
    print(f'Historical data load completed in {int(time.time()) - start_time} seconds')
