from collections.abc import Callable
from dataclasses import dataclass

from delta._typing import ColumnMapping
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass(frozen=True)
class MergeSpec:
    join_on: str
    insert: bool = True  # When not matched then insert
    update: bool = False  # when matched (and cond) then update
    update_condition: str | None = None
    update_set: ColumnMapping | None = None  # None means all cols (set *)


def merge_batch(spark: SparkSession, target_table: str, spec: MergeSpec) -> Callable:
    """Return the callable for a batch merge (micro-batch)."""

    def _upsert(batch_df: DataFrame, batch_id: int) -> None:
        builder = (
            DeltaTable.forName(spark, target_table).alias('t').merge(source=batch_df.alias('s'), condition=spec.join_on)
        )
        if spec.update:
            builder = (
                builder.whenMatchedUpdate(set=spec.update_set, condition=spec.update_condition)
                if spec.update_set
                else builder.whenMatchedUpdateAll(condition=spec.update_condition)
            )
        if spec.insert:
            builder = builder.whenNotMatchedInsertAll()
        builder.execute()

    return _upsert


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
