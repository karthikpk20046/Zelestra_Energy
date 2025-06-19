from pyspark.sql.functions import col, from_unixtime, to_timestamp
from database import read_table_from_postgres
from filters import filter_recent_tables, get_matching_tag_ids
from io_utils import append_and_upload_parquet
import datetime
import json

def run_pipeline():
    from database import create_spark_session, load_db_credentials
    import os

    with open("config/parameters.json") as f:
        config = json.load(f)

    spark = create_spark_session()
    db_conf = load_db_credentials(config["db_credentials_path"])

    # Read tag table and all table names
    tags_df = read_table_from_postgres(spark, db_conf, config['tags_table'])
    all_tables_df = read_table_from_postgres(spark, db_conf, "information_schema.tables")
    table_names = [row[2] for row in all_tables_df.select("table_schema", "table_name").collect() if row[0] == "public"]

    # Filter table names based on year/month
    filtered_tables = filter_recent_tables(table_names, config['start_year'], config['start_month'])

    # Get tag IDs from patterns
    tag_ids = get_matching_tag_ids(tags_df, config['patterns'])
    ids_str = ','.join(map(str, tag_ids))

    # Calculate Unix timestamp for 30 days ago
    now = datetime.datetime.now()
    thirty_days_ago = now - datetime.timedelta(days=30)
    unix_ms = int(thirty_days_ago.timestamp() * 1000)

    for table in filtered_tables:
        query = f"(SELECT * FROM {table} WHERE tagid IN ({ids_str}) AND t_stamp >= {unix_ms}) AS subq"
        df = read_table_from_postgres(spark, db_conf, query)
        df = df.join(tags_df, df.tagid == tags_df.id, "left")
        df = df.filter(col("dataintegrity") != 0)

        df = df.withColumn("datetime", to_timestamp(from_unixtime(col("t_stamp") / 1000)))
        df = df.withColumn("datetime", col("datetime").cast("timestamp"))

        for tagpath in df.select("tagpath").distinct().rdd.flatMap(lambda x: x).collect():
            tag_data = df.filter(col("tagpath") == tagpath)
            value_col = [c for c in tag_data.columns if c not in ["tagid", "id", "dataintegrity", "tagpath", "t_stamp", "datetime"]][0]
            tag_name = tagpath.replace('/', '_')
            final_df = tag_data.select("datetime", col(value_col).alias(tag_name))
            final_df = final_df.dropna()

            path_s3 = f"s3://{config['bucket_name']}/{config['directory_s3']}{tag_name}.parquet"
            append_and_upload_parquet(spark, path_s3, final_df, path_s3)
