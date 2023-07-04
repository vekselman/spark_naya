from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer, KafkaConsumer
import threading

spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0')\
    .appName("data_enrichment") \
    .getOrCreate()

fs = "hdfs://course-hdfs:8020"

bootstrap_servers = "course-kafka:9092"
input_topic = "sensors-sample"
output_topic = "samples-enriched"
cars_input = "/data/dims/cars"
models_input = "/data/dims/car_models"
colors_input = "/data/dims/car_colors"


def enriche():
    models = spark.read.parquet(fs + models_input)
    colors = spark.read.parquet(fs + colors_input)
    cars = spark.read.parquet(fs + cars_input)
    df = cars.join(colors, on="color_id").join(models, on="car_model")

    schema = T.StructType()\
        .add("event_id", T.StringType())\
        .add("event_time", T.TimestampType())\
        .add("car_id", T.IntegerType())\
        .add("speed", T.IntegerType())\
        .add("rpm", T.IntegerType())\
        .add("gear", T.IntegerType())

    stream_df = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", bootstrap_servers)\
        .option("subscribe", input_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    stream_df = stream_df.select(
            F.from_json(F.col("value").cast(
                T.StringType()), schema
            ).alias("data"))\
        .select("data.*")

    stream_df.writeStream\
        .format("console") \
        .option("truncate", False)\
        .outputMode("append")\
        .start()\
        .awaitTermination()


if __name__ == "__main__":
    try:
        enriche()
    finally:
        spark.stop()
