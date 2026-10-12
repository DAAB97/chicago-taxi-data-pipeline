from pyspark.sql import functions as F
from common import spark_session,lake_path
s=spark_session("quality-checks"); d=s.read.parquet(lake_path("silver/chicago_taxi_trips")); m=d.agg(F.count("*").alias("rows"),F.sum(F.col("trip_id").isNull().cast("int")).alias("nulls"),F.countDistinct("trip_id").alias("distinct_ids"),F.sum((F.col("revenue")<0).cast("int")).alias("negative")).first()
assert m.rows>0 and m.nulls==0 and m.rows==m.distinct_ids and m.negative==0
for p in ["daily_kpis","top_pickup_zones","payment_summary"]: assert s.read.parquet(lake_path(f"gold/{p}")).count()>0
s.stop()
