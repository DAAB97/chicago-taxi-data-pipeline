import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from src.download_to_bronze import run_download
endpoint=os.getenv("S3_ENDPOINT","http://seaweedfs-s3:8333"); key=os.getenv("S3_ACCESS_KEY","taxi_access_key"); secret=os.getenv("S3_SECRET_KEY","taxi_secret_key_change_me"); bucket=os.getenv("S3_BUCKET","datalake")
conf={"spark.hadoop.fs.s3a.endpoint":endpoint,"spark.hadoop.fs.s3a.access.key":key,"spark.hadoop.fs.s3a.secret.key":secret,"spark.hadoop.fs.s3a.path.style.access":"true","spark.hadoop.fs.s3a.connection.ssl.enabled":"false","spark.hadoop.fs.s3a.endpoint.region":"us-east-1","spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","spark.jars":"/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar"}
env={"S3_ENDPOINT":endpoint,"S3_ACCESS_KEY":key,"S3_SECRET_KEY":secret,"S3_BUCKET":bucket}
def st(i,a): return SparkSubmitOperator(task_id=i,application=a,conn_id="spark_default",conf=conf,env_vars=env,verbose=True)
with DAG("chicago_taxi_medallion",start_date=datetime(2023,1,1),schedule=None,catchup=False,default_args={"owner":"data-engineering","retries":2,"retry_delay":timedelta(minutes=2)},tags=["spark","seaweedfs","s3","medallion"]) as dag:
 ingest=PythonOperator(task_id="ingest_soda_to_bronze",python_callable=run_download); silver=st("transform_bronze_to_silver","/opt/spark/jobs/bronze_to_silver.py"); gold=st("build_gold_aggregates","/opt/spark/jobs/silver_to_gold.py"); quality=st("run_quality_checks","/opt/spark/jobs/quality_checks.py"); ingest>>silver>>gold>>quality
