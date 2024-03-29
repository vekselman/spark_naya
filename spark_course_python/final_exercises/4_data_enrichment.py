from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


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
checkpoint_location = "/data/dims/checkpoints/enrichment"

DEBUG = False


def enriche():
    models = spark.read.parquet(fs + models_input)
    colors = spark.read.parquet(fs + colors_input)
    cars = spark.read.parquet(fs + cars_input)
    df = cars.alias("cars")\
        .join(colors.alias("colors"), on="color_id")\
        .join(models.alias("models"), cars["car_model"] == models["model_id"])

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

    stream_df = stream_df.join(df, on="car_id")\
        .select([
            "event_id",
            "event_time",
            "car_id",
            "speed",
            "rpm",
            "gear",
            "driver_id",
            F.col("car_brand").alias("brand_name"),
            F.col("models.car_model").alias("model_name"),
            "color_name",
            F.abs(F.col("speed") / 30).cast(T.IntegerType()).alias("expected_gear")
        ])

    if DEBUG:
        stream_df.writeStream\
            .format("console") \
            .option("truncate", False)\
            .outputMode("append")\
            .start()\
            .awaitTermination()
    else:
        stream_df.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value")\
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
        enriche()
    finally:
        spark.stop()
