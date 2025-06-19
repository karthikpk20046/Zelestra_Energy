import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from pyspark.sql import DataFrame


def upload_parquet_to_s3(df: DataFrame, path: str):
    """Write PySpark DataFrame to S3 in parquet format."""
    df.write.mode("overwrite").parquet(path)


def append_and_upload_parquet(spark, existing_path: str, new_df: DataFrame, write_path: str):
    """Read old data if exists and append new data, then write back to S3."""
    try:
        existing_df = spark.read.parquet(existing_path)
        combined_df = existing_df.unionByName(new_df).dropDuplicates(["datetime"]).orderBy("datetime")
    except Exception:
        combined_df = new_df

    combined_df.write.mode("overwrite").parquet(write_path)
