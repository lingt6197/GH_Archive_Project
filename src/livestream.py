from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, MapType, LongType


def create_stream_spark():
    spark = SparkSession.builder.appName("gh-archive-livestream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_schema():
    # Definition of GH Archive Schema
    return StructType(
        [
            StructField("type", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("actor", MapType(StringType(), StringType()), True),
            StructField("repo", MapType(StringType(), StringType()), True),
            StructField(
                "payload", MapType(StringType(), StringType()), True
            ),  # 简化处理
        ]
    )


if __name__ == "__main__":
    spark = create_stream_spark()
    schema = get_schema()

    # Acquisition on folder changes
    streaming_df = spark.readStream.schema(schema).json("stream_data/")

    # Transformation
    # filter and extract
    clean_stream = (
        streaming_df.filter(col("type") == "PushEvent")
        .select(
            to_timestamp(col("created_at")).alias("timestamp"),
            col("actor.login").alias("user"),
            col("repo.name").alias("repo"),
        )
        .dropna(subset=["timestamp"])
    )

    # time window aggregation
    windowed_counts = (
        clean_stream.withWatermark("timestamp", "10 minutes")
        .groupBy(window(col("timestamp"), "1 hour"))
        .count()
    )

    # TD: goes to parquet or DB
    query = (
        windowed_counts.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()
