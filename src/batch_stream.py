from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, to_timestamp, year, month, dayofmonth, hour

def create_spark(app_name="gh-archive-stream"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def load_data(spark, path="data/*.json.gz"):
    return spark.read.json(path)


def clean_data(df):
    df.filter(col("type") == "PushEvent")
    df.select(col("created_at"),
              col("actor.login").alias("user"),
              col("repo.name").alias("repo"),
              col("payload.size").alias("commit_count"))
    df.withColumn("timestamp", to_timestamp("created_at")) \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp")) \
        .withColumn("hour", hour("timestamp")).dropna(subset=["timestamp"])
    return df


def aggregate_pushevents(df):
    df.groupBy("year","month","day","hour").agg(count("*").alias("push_event_count"))


def write_parquet_files(batch_df, path):
    batch_df.write.mode("overwrite").parquet(path)

    