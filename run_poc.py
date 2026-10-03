import sys
from poc.spark import get_spark
from poc.make_data import make_rows
from poc.transform import transform_df

business_date = sys.argv[1] if len(sys.argv) > 1 else "2026-10-04"

spark = get_spark()
source = spark.createDataFrame(make_rows())
source.write.mode("overwrite").parquet("data/pay_chase_pymt_auth")   # the "upstream" input

source = spark.read.parquet("data/pay_chase_pymt_auth")
result = transform_df(source, business_date)

print(f"Input rows:  {source.count()}")
print(f"Output rows: {result.count()}  (one per payment id)")
result.printSchema()
result.show(10, truncate=False)

result.write.mode("overwrite").parquet("output/mms_audit_xfradt_2day_paychase_sub")
