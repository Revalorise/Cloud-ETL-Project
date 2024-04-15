from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from pendulum import datetime
import pandas as pd


bucket_name = "nyc-yellow-taxi-data"
file_name = "yellow_tripdata_2024-01.csv"


@dag(
    dag_id="cloud_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "airflow", "retries": 3},
    tags=['cloud_ingestion'],
)
def cloud_ingestion():
    
    download_data = BashOperator(
        task_id='download_data',
        bash_command="curl -o ./data https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )

    @task(task_id="parquet_to_csv")
    def parquet_to_csv():
        """
        This task converts the downloaded data format form .parquet to .csv
        """
        downloaded_data = "./data/yellow_tripdata_2024-01.parquet"
        df = pd.read_csv(downloaded_data)
        df.to_csv("./data" + downloaded_data.split("/")[-1].replace(".parquet", ".csv"), index=False)

    @task(task_id="transform_data")
    def transform_data():
        """
        This task cleans the downloaded data by changing the column data types to a proper format
        """
        df = pd.read_csv("./data/yellow_tripdata_2024-01.csv")
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name=bucket_name,
        aws_conn_id="aws_default",
        region_name="us-east-2",
    )

    create_gcp_bucket = GCSCreateBucketOperator(
        task_id="create_gcp_bucket",
        bucket_name=bucket_name,
        storage_class="STANDARD",
        location="US",
        project_id="clever-environs-419521",
        gcp_conn_id="google_cloud_default",
    )

    upload_data_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_data_to_s3",
        filename=f"./data/{file_name}",
        dest_key=file_name,
        dest_bucket=bucket_name,
        aws_conn_id="aws_default",
        replace="True",
        encrypt="False",
        gzip="False",
    )

    upload_data_to_gcp = LocalFilesystemToGCSOperator(
        task_id="upload_data_to_gcp",
        src=f"./data/{file_name}",
        dst=file_name,
        bucket=bucket_name,
        gcp_conn_id="google_cloud_default",
        mime_type="application/octet-stream",
        gzip="False",
    )

    chain(download_data, parquet_to_csv(), transform_data(),
          [create_s3_bucket, create_gcp_bucket], [upload_data_to_s3, upload_data_to_gcp])


cloud_ingestion()
