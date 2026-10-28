from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def dedup_latest(id_col: str, order_col: str) -> Callable:
    """Deduplicate CDC micro-batches, keeping latest record.

    Args:
      id_col: The column name used to partition the records (identifier).
      order_col: The column name used to determine the latest record in
        descending order.

    Returns:
        A transformation function that takes a DataFrame and returns a
        deduplicated DataFrame.
    """

    def _fn(df: DataFrame) -> DataFrame:
        w = Window.partitionBy(id_col).orderBy(F.col(order_col).desc())
        return (
            df.filter(F.col('update_type').isin(['new', 'update']))
            .withColumn('_rank', F.rank().over(w))
            .filter('_rank == 1')
            .drop('_rank')
        )

    return _fn
