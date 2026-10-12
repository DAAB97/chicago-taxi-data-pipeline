import csv, io, os, time
from datetime import datetime, timezone
import boto3, requests
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
FIELDS=["trip_id","taxi_id","trip_start_timestamp","trip_end_timestamp","trip_seconds","trip_miles","pickup_census_tract","dropoff_census_tract","pickup_community_area","dropoff_community_area","fare","tips","tolls","extras","trip_total","payment_type","company","pickup_centroid_latitude","pickup_centroid_longitude","dropoff_centroid_latitude","dropoff_centroid_longitude"]
def s3_client():
 return boto3.client("s3",endpoint_url=os.getenv("S3_ENDPOINT","http://seaweedfs-s3:8333"),aws_access_key_id=os.getenv("S3_ACCESS_KEY","taxi_access_key"),aws_secret_access_key=os.getenv("S3_SECRET_KEY","taxi_secret_key_change_me"),region_name="us-east-1",config=Config(s3={"addressing_style":"path"},retries={"max_attempts":5,"mode":"standard"}))
def ensure_bucket(c,b):
 for i in range(30):
  try:
   try:c.head_bucket(Bucket=b)
   except ClientError:c.create_bucket(Bucket=b)
   return
  except (ClientError,EndpointConnectionError):
   if i==29: raise
   time.sleep(2)
def run_download():
 dataset=os.getenv("DATASET_ID","wrvz-psew"); start=os.getenv("START_DATE","2023-01-01T00:00:00.000"); end=os.getenv("END_DATE","2023-02-01T00:00:00.000")
 page_size=int(os.getenv("PAGE_SIZE","50000")); max_rows=int(os.getenv("MAX_ROWS","500000")); bucket=os.getenv("S3_BUCKET","datalake")
 headers={"User-Agent":"artefact-data-engineer-test/1.0"}
 if os.getenv("SODA_APP_TOKEN"): headers["X-App-Token"]=os.environ["SODA_APP_TOKEN"]
 c=s3_client(); ensure_bucket(c,bucket); total=part=0; run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
 while total<max_rows:
  limit=min(page_size,max_rows-total); params={"$select":",".join(FIELDS),"$where":f"trip_start_timestamp >= '{start}' AND trip_start_timestamp < '{end}'","$order":"trip_start_timestamp,trip_id","$limit":limit,"$offset":total}
  r=requests.get(f"https://data.cityofchicago.org/resource/{dataset}.csv",params=params,headers=headers,timeout=180); r.raise_for_status(); rows=list(csv.DictReader(io.StringIO(r.text)))
  if not rows: break
  out=io.StringIO(); w=csv.DictWriter(out,fieldnames=FIELDS); w.writeheader(); w.writerows(rows)
  c.put_object(Bucket=bucket,Key=f"bronze/chicago_taxi/period_start={start[:10]}/run_id={run_id}/part-{part:05d}.csv",Body=out.getvalue().encode(),ContentType="text/csv")
  total+=len(rows); part+=1
  if len(rows)<limit: break
 if not total: raise ValueError("Aucune ligne telechargee")
 return {"rows":total,"parts":part,"run_id":run_id}
