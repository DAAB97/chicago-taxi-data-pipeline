from pyspark.sql import functions as F, types as T
from common import spark_session,lake_path
s=spark_session("bronze-to-silver")
cols=["trip_id","taxi_id","trip_start_timestamp","trip_end_timestamp","trip_seconds","trip_miles","pickup_census_tract","dropoff_census_tract","pickup_community_area","dropoff_community_area","fare","tips","tolls","extras","trip_total","payment_type","company","pickup_centroid_latitude","pickup_centroid_longitude","dropoff_centroid_latitude","dropoff_centroid_longitude"]
d=s.read.option("header",True).schema(T.StructType([T.StructField(c,T.StringType()) for c in cols])).csv(lake_path("bronze/chicago_taxi/"))
for c in ["trip_start_timestamp","trip_end_timestamp"]: d=d.withColumn(c,F.to_timestamp(c))
for c in ["trip_seconds","trip_miles","fare","tips","tolls","extras","trip_total","pickup_centroid_latitude","pickup_centroid_longitude","dropoff_centroid_latitude","dropoff_centroid_longitude"]: d=d.withColumn(c,F.col(c).cast("double"))
d=(d.dropDuplicates(["trip_id"]).filter(F.col("trip_id").isNotNull()&F.col("trip_start_timestamp").isNotNull()).filter((F.col("trip_seconds")>=0)&(F.col("trip_miles")>=0)&(F.col("trip_total")>=0)).withColumn("trip_date",F.to_date("trip_start_timestamp")).withColumn("duration_minutes",F.col("trip_seconds")/60).withColumn("revenue",F.col("trip_total")).withColumn("pickup_zone",F.coalesce("pickup_community_area",F.lit("UNKNOWN"))))
d.write.mode("overwrite").partitionBy("trip_date").parquet(lake_path("silver/chicago_taxi_trips")); s.stop()
