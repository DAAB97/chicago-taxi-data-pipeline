import os
from pyspark.sql import SparkSession
def spark_session(name):
 s=SparkSession.builder.appName(name).getOrCreate(); h=s.sparkContext._jsc.hadoopConfiguration()
 for k,v in {"fs.s3a.endpoint":os.getenv("S3_ENDPOINT","http://seaweedfs-s3:8333"),"fs.s3a.access.key":os.getenv("S3_ACCESS_KEY","taxi_access_key"),"fs.s3a.secret.key":os.getenv("S3_SECRET_KEY","taxi_secret_key_change_me"),"fs.s3a.path.style.access":"true","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint.region":"us-east-1","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem"}.items(): h.set(k,v)
 return s
def lake_path(p): return f"s3a://{os.getenv('S3_BUCKET','datalake')}/{p.lstrip('/')}"
