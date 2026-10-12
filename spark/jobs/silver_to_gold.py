from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import spark_session,lake_path
s=spark_session("silver-to-gold"); d=s.read.parquet(lake_path("silver/chicago_taxi_trips"))
d.groupBy("trip_date").agg(F.count("*").alias("trip_count"),F.round(F.sum("revenue"),2).alias("total_revenue"),F.round(F.avg("duration_minutes"),2).alias("avg_duration_minutes"),F.round(F.avg("trip_miles"),2).alias("avg_trip_miles"),F.round(F.avg("revenue"),2).alias("avg_revenue_per_trip")).write.mode("overwrite").parquet(lake_path("gold/daily_kpis"))
z=d.groupBy("trip_date","pickup_zone").agg(F.count("*").alias("trip_count"),F.round(F.sum("revenue"),2).alias("total_revenue")).withColumn("rank",F.row_number().over(Window.partitionBy("trip_date").orderBy(F.desc("trip_count")))).filter("rank<=10")
z.write.mode("overwrite").partitionBy("trip_date").parquet(lake_path("gold/top_pickup_zones"))
d.groupBy("payment_type").agg(F.count("*").alias("trip_count"),F.round(F.sum("revenue"),2).alias("total_revenue")).write.mode("overwrite").parquet(lake_path("gold/payment_summary")); s.stop()
