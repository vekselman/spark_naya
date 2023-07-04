from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer
import uuid
import time


spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
    .appName("data_generator") \
    .getOrCreate()

fs = "hdfs://course-hdfs:8020"

bootstrap_servers = "course-kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: v.encode("utf-8")
)


def sensors():
    output_topic = "sensors-sample"
    cars_input = "/data/dims/cars"

    schema = T.StructType([
        T.StructField("event_id", T.StringType(), True),
        T.StructField("event_time", T.TimestampType(), True),
        T.StructField("car_id", T.IntegerType(), True),
        T.StructField("speed", T.IntegerType(), True),
        T.StructField("rpm", T.IntegerType(), True),
        T.StructField("gear", T.IntegerType(), True)
    ])

    df = spark.createDataFrame(data=[], schema=schema)
    cars = spark.read.parquet(fs + cars_input)\
        .select("car_id")

    df = df.join(cars, on="car_id", how="right")

    for i in range(3):
        df_new = df.select([
            F.concat(F.col("car_id"), F.lit("_"), F.current_timestamp().cast("integer")).cast("string").alias("event_id"),
            F.current_timestamp().cast("timestamp").alias("event_time"),
            "car_id",
            (F.rand() * 200).cast("integer").alias("speed"),
            (F.rand() * 8000).cast("integer").alias("rpm"),
            (F.rand() * 7 + 1).cast("integer").alias("gear")
        ])
        # df_new.show(truncate=False)
        # df_new.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value")\
        #     .show(truncate=False)
        # break
        df_new.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value")\
            .write\
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", output_topic)\
            .save()
        time.sleep(1)


if __name__ == "__main__":
    try:
        sensors()
    finally:
        producer.close()
        spark.stop()
