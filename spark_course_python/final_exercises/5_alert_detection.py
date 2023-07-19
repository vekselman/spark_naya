from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window


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

DEBUG = True


def main():

    schema = T.StructType() \
        .add("event_id", T.StringType()) \
        .add("event_time", T.TimestampType()) \
        .add("car_id", T.IntegerType()) \
        .add("speed", T.IntegerType()) \
        .add("rpm", T.IntegerType()) \
        .add("gear", T.IntegerType())\
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
        alert_df = df\
            .where(
                (F.col("speed") > 120) |
                (F.col("gear") != F.col("expected_gear")) |
                (F.col("rpm") > 6000)
            )\
            .orderBy("car_id", "event_time")

        # anomaly_df = df \
        #     .withWatermark("event_time", "10 seconds") \
        #     .groupBy(F.col('car_id'), F.window("event_time", "10 seconds", "1 second")) \
        #     .agg(F.avg(F.col("speed")).alias("avg_speed"),
        #          F.avg(F.col("rpm")).alias("avg_rpm"),
        #          F.avg(F.col("gear")).alias("avg_gear")
        #          ) \
        #     .orderBy("car_id", "window")\
        #     .withColumn("avg_gear_lag", F.expr("lag(avg_gear) over (order by window)")) \
        #     .withColumn("avg_speed_lag", F.expr("lag(avg_speed) over (order by window)")) \
        #     .withColumn("avg_rpm_lag", F.expr("lag(avg_rpm) over (order by window)"))\
        #     .orderBy("car_id", "window")

        window_spec = Window.partitionBy('car_id').orderBy('event_time').rangeBetween(-10, 0)

        df_with_avg = df \
            .orderBy('event_time')\
            .withWatermark("event_time", "10 seconds") \
            .groupBy('car_id', F.window('event_time', "10 seconds", "1 second")) \
            .agg(
                F.avg('gear').alias('avg_gear'),
                F.avg('speed').alias('avg_speed'),
                F.avg('rpm').alias('avg_rpm')
            )
        df_with_avg_lagged = df_with_avg\
            .withColumn('lag_avg_gear', F.expr("lag(avg_gear) over (order by car_id, window)")) \
            .withColumn('lag_avg_speed', F.expr("lag(avg_speed) over (order by car_id, window)")) \
            .withColumn('lag_avg_rpm', F.expr("lag(avg_rpm) over (order by car_id, window)"))

        anomaly_df = df_with_avg_lagged\
            .where(
                ((F.col("avg_gear") - F.col('lag_avg_gear')) > 1) |
                ((F.col('avg_speed') > (F.col('lag_avg_speed') * 1.1)) & (F.col('avg_speed') > 30)) |
                ((F.col('avg_rpm') > (F.col('lag_avg_rpm') * 1.2)) & (F.col('avg_rpm') > 500))
            )


        if DEBUG:
            # alert_df.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value") \
            alert_df \
                .write \
                .format("console") \
                .option("truncate", False) \
                .save()
            df \
                .orderBy("car_id", "event_time")\
                .write \
                .format("console") \
                .option("truncate", False) \
                .save()
            anomaly_df\
                .write\
                .format("console") \
                .option("truncate", False) \
                .save()
        else:
            alert_df \
                .write \
                .format("console") \
                .option("truncate", False) \
                .save()
            anomaly_df \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", bootstrap_servers) \
                .option("checkpointLocation", checkpoint_location + "/alert_detection") \
                .option("truncate", False) \
                .save()
            #         alert_df.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value") \
            # .write \
            # .format("kafka") \
            # .option("kafka.bootstrap.servers", bootstrap_servers) \
            # .option("checkpointLocation", checkpoint_location + "/alert_detection") \
            # .option("topic", alert_topic) \
            # .save()

        # anomaly_df.selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        #     .write \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", bootstrap_servers) \
        #     .option("checkpointLocation", checkpoint_location + "/anomaly_detection") \
        #     .option("topic", anomaly_topic) \
        #     .save()
    stream_df.writeStream \
        .foreachBatch(write_to_kafka_topic) \
        .start() \
        .awaitTermination()
    # if DEBUG:
    #     stream_df.writeStream \
    #         .format("console") \
    #         .option("truncate", False) \
    #         .outputMode("append") \
    #         .start() \
    #         .awaitTermination()
    # else:
    #     stream_df.writeStream\
    #         .foreachBatch(write_to_kafka_topic)\
    #         .start()\
    #         .awaitTermination()
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
