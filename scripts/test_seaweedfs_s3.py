import os,time,uuid,boto3
from botocore.config import Config
c=boto3.client("s3",endpoint_url=os.getenv("S3_ENDPOINT"),aws_access_key_id=os.getenv("S3_ACCESS_KEY"),aws_secret_access_key=os.getenv("S3_SECRET_KEY"),region_name="us-east-1",config=Config(s3={"addressing_style":"path"})); b=os.getenv("S3_BUCKET","datalake")
for i in range(60):
 try: c.list_buckets(); break
 except Exception:
  if i==59: raise
  time.sleep(2)
try:c.head_bucket(Bucket=b)
except Exception:c.create_bucket(Bucket=b)
k=f"smoke-tests/{uuid.uuid4()}.txt"; body=b"seaweedfs-s3-ok"; c.put_object(Bucket=b,Key=k,Body=body); assert c.get_object(Bucket=b,Key=k)["Body"].read()==body; assert any(x["Key"]==k for x in c.list_objects_v2(Bucket=b,Prefix="smoke-tests/").get("Contents",[])); c.delete_object(Bucket=b,Key=k); print("OK: create, put, get, list et delete")
