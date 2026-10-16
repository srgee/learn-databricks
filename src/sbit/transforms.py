import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def add_audit_columns(df: DataFrame) -> DataFrame:
    return df.withColumn('created_at', F.current_timestamp()).withColumn('source_file', F.input_file_name())


def join_date_lookup(df: DataFrame) -> DataFrame:
    lookup = df.sparkSession.table('date_lookup').select('date', 'week_part')
    cond = F.to_date((F.col('timestamp') / 1000).cast('timestamp')) == F.col('date')
    return df.join(F.broadcast(lookup), cond, 'left')
