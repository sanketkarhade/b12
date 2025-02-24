from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import days_ago

# Define DAG parameters
dag = DAG(
    "s3_upload_dag",
    default_args={"owner": "airflow"},
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
)

# Upload file to S3
upload_task = LocalFilesystemToS3Operator(
    task_id="upload_file_to_s3",
    filename="/path/to/local/file.txt",  # Change this to the actual file path
    bucket_name="your-s3-bucket-name",   # Replace with your S3 bucket name
    key="uploaded_file.txt",             # Name of the file in S3
    aws_conn_id="aws_default",
    replace=True,
    dag=dag,
)
