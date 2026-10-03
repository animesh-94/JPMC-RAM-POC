from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, ShortType, StringType

def payment_type_code():
    return (F.when(F.col("FUND_EXTN_ACCT_IN") == "Y", F.lit(6))
             .when(F.col("FUND_EXTN_ACCT_IN") == "N", F.lit(8))).cast(ShortType())

def get_column_map():
    return [
        ("pymt_id",      F.col("pymt_id").cast(StringType())),
        ("pymt_amt",     F.col("pymt_amt").cast(DecimalType(18, 2))),
        ("updt_dt",      F.to_date("updt_ts")),
        ("pymt_type_cd", payment_type_code()),
        ("unused_col",   F.lit(None).cast(StringType())),
    ]
