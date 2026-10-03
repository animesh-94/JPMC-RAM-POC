from pyspark.sql import DataFrame, Window, functions as F
from .mapping import get_column_map

def dedupe_latest(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("pymt_id").orderBy(F.col("updt_ts").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn"))

def map_columns(df: DataFrame) -> DataFrame:
    return df.select(*[expr.alias(name) for name, expr in get_column_map()])

def add_audit_columns(df: DataFrame, business_date: str) -> DataFrame:
    return (df.withColumn("extract_transform_load_timestamp", F.current_timestamp())
              .withColumn("effective_date", F.to_date(F.lit(business_date))))

def transform_df(source_df: DataFrame, business_date: str) -> DataFrame:
    return add_audit_columns(map_columns(dedupe_latest(source_df)), business_date)
