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
alert_topic = "alert-data"
anomaly_topic = "anomaly-alerts"

checkpoint_location = "/data/dims/checkpoints"

DEBUG = False


def main():

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

    def write_to_kafka_topic(df, epoch_id):
        alert_df = df \
            .where(
            (F.col("speed") > 120) |
            (F.col("gear") != F.col("expected_gear")) |
            (F.col("rpm") > 6000)
        )

        anomaly_df = df \
            .withWatermark("event_time", "10 seconds") \
            .groupBy(F.window("event_time", "10 seconds", "1 second")) \
            .agg(F.avg(F.col("speed")).alias("avg_speed"),
                 F.avg(F.col("rpm")).alias("avg_rpm"),
                 F.avg(F.col("gear")).alias("avg_gear")
                 ) \
            .withColumn("avg_gear_lag", F.lag(F.col("gear"))expr("lag(avg_gear) over (order by window)")) \
                .withColumn("avg_speed_lag", expr("lag(avg_speed) over (order by window)")) \
            .withColumn("avg_rpm_lag", expr("lag(avg_rpm) over (order by window)")) \
            .where(

            alert_df.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", bootstrap_servers) \
                .option("checkpointLocation", checkpoint_location + "/alert_detection") \
                .option("topic", alert_topic) \
                .save()

        anomaly_df.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("checkpointLocation", checkpoint_location + "/anomaly_detection") \
            .option("topic", anomaly_topic) \
            .save()


        if DEBUG:
            stream_df.writeStream \
                .format("console") \
                .option("truncate", False) \
                .outputMode("append") \
                .start() \
                .awaitTermination()
        else:
            stream_df.writeStream \
                .foreachBatch(write_to_kafka_topic) \
                .start() \
                .awaitTermination()
        # stream_df.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        #     .writeStream \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", bootstrap_servers) \
        #     .option("topic", output_topic) \
        #     .option("checkpointLocation", fs + checkpoint_location) \
        #     .outputMode("update") \
        #     .start() \
        #     .awaitTermination()


        if __name__ == "__main__":
        try:
            main()
        finally:
            spark.stop()



### FROM chatgpt
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, expr

# Create SparkSession
spark = SparkSession.builder.appName("AnomalyDetection").getOrCreate()

# Configure Kafka connection
bootstrap_servers = 'localhost:9092'
topic = 'your_topic'

# Create a streaming DataFrame
streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .load()

# Apply transformations to extract necessary columns and cast them to appropriate data types
parsed_df = streaming_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .selectExpr("CAST(key AS INT) as car_id", "CAST(value AS STRING) as json_data") \
    .selectExpr("car_id", "from_json(json_data, 'car_id INT, driver_id INT, car_model INT, speed INT, rpm INT, gear INT') as data") \
    .selectExpr("car_id", "data.driver_id", "data.car_model", "data.speed", "data.rpm", "data.gear")

# Apply sliding window of 10 seconds with a slide duration of 1 second
windowed_df = parsed_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(window("event_time", "10 seconds", "1 second")) \
    .agg(expr("avg(speed) as avg_speed"), expr("avg(rpm) as avg_rpm"), expr("avg(gear) as avg_gear"))

# Define the anomaly detection conditions
anomaly_df = windowed_df \
    .withColumn("avg_gear_lag", expr("lag(avg_gear) over (order by window)")) \
    .withColumn("avg_speed_lag", expr("lag(avg_speed) over (order by window)")) \
    .withColumn("avg_rpm_lag", expr("lag(avg_rpm) over (order by window)")) \
    .where(
    (expr("avg_gear - avg_gear_lag >= 1")) |
    (expr("avg_speed - avg_speed_lag >= 0.1 * avg_speed_lag AND avg_speed - avg_speed_lag >= 30")) |
    (expr("avg_rpm - avg_rpm_lag >= 0.2 * avg_rpm_lag AND avg_rpm - avg_rpm_lag >= 500"))
)

# Write the anomaly DataFrame to console
query = anomaly_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to finish
query.awaitTermination()
