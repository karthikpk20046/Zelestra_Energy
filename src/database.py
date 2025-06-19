from pyspark.sql import SparkSession
import json
from urllib.parse import quote_plus

def create_spark_session(app_name="DE_Assessment_App"):
    """Create and return a SparkSession object."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_db_credentials(path):
    """Load DB credentials from a JSON file."""
    with open(path, 'r') as f:
        return json.load(f)['YourDB']

def read_table_from_postgres(spark, db_conf, table, predicates=None):
    """Read a table from PostgreSQL using JDBC.

    Args:
        spark (SparkSession): The Spark session
        db_conf (dict): Contains keys 'user', 'password', 'host', 'dbname'
        table (str): Table name to query
        predicates (list): Optional list of predicates (pushdown filtering)
    Returns:
        DataFrame: Spark DataFrame of the queried table
    """
    url = f"jdbc:postgresql://{db_conf['host']}/{db_conf['dbname']}"
    properties = {
        "user": db_conf['user'],
        "password": db_conf['password'],
        "driver": "org.postgresql.Driver"
    }
    if predicates:
        return spark.read.jdbc(url=url, table=table, properties=properties, predicates=predicates)
    else:
        return spark.read.jdbc(url=url, table=table, properties=properties)
