from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Single place to configure Spark for all batch jobs.
    Why: every job needs same config — write once, use everywhere.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()