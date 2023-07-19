from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName("cars_creation") \
    .getOrCreate()

fs = "hdfs://course-hdfs:8020"


def cars():
    output = "/data/dims/cars"
    schema = T.StructType([
        T.StructField("car_id", T.IntegerType(), False),
        T.StructField("driver_id", T.IntegerType(), False),
        T.StructField("car_model", T.IntegerType(), False),
        T.StructField("color_id", T.IntegerType(), False)
    ])

    df = spark.range(20).select(
        ((F.rand() * 0.9 + 0.1) * 10**7).cast("integer").alias("car_id"),
        ((F.rand() * 0.9 + 0.1) * 10**9).cast("integer").alias("driver_id"),
        ((F.rand() * 0.9 + 0.1) * 7 + 1).cast("integer").alias("car_model"),
        ((F.rand() * 0.9 + 0.1) * 7 + 1).cast("integer").alias("color_id")
    )
    df = df.dropDuplicates()

    df.coalesce(1) \
        .write \
        .parquet(path=fs + output,
                 mode="overwrite")


if __name__ == "__main__":
    try:
        cars()
    finally:
        spark.stop()
