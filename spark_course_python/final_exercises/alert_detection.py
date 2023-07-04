from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
    .appName("data_enrichment") \
    .getOrCreate()

fs = "hdfs://course-hdfs:8020"

bootstrap_servers = "course-kafka:9092"
input_topic = "samples-enriched"
output_topic = "alert-data"

checkpoint_location = "/data/dims/checkpoints/alert_detection"

DEBUG = False


def alert():

    schema = T.StructType() \
        .add("event_id", T.StringType()) \
        .add("event_time", T.TimestampType()) \
        .add("car_id", T.IntegerType()) \
        .add("speed", T.IntegerType()) \
        .add("rpm", T.IntegerType()) \
        .add("gear", T.IntegerType()) \
        .add("driver_id", T.IntegerType()) \
        .add("brand_name", T.StringType()) \
        .add("model_name", T.StringType()) \
        .add("color_name", T.StringType()) \
        .add("expected_gear", T.IntegerType())

    stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    stream_df = stream_df.select(
        F.from_json(F.col("value").cast(
            T.StringType()), schema
        ).alias("data")) \
        .select("data.*")

    stream_df = stream_df \
        .where(
        (F.col("speed") > 120) |
        (F.col("gear") != F.col("expected_gear")) |
        (F.col("rpm") > 6000)
    )

    if DEBUG:
        stream_df.writeStream \
            .format("console") \
            .option("truncate", False) \
            .outputMode("append") \
            .start() \
            .awaitTermination()
    else:
        stream_df.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", output_topic) \
            .option("checkpointLocation", fs + checkpoint_location) \
            .outputMode("update") \
            .start() \
            .awaitTermination()


if __name__ == "__main__":
    try:
        alert()
    finally:
        spark.stop()
