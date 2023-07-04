from pyspark.sql import SparkSession
from pyspark.sql import types as T

spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName("model_creation") \
    .getOrCreate()

fs = "hdfs://course-hdfs:8020"


def car_models():
    output = "/data/dims/car_models"
    schema = T.StructType([
        T.StructField("model_id", T.IntegerType(), False),
        T.StructField("car_brand", T.StringType(), True),
        T.StructField("car_model", T.StringType(), True)
    ])
    data = [
        [1, "Mazda", "3"],
        [2, "Mazda", "6"],
        [3, "Toyota", "Corolla"],
        [4, "Hyundai", "i20"],
        [5, "Kia", "Sportage"],
        [6, "Kia", "Rio"],
        [7, "Kia", "Picanto"]
    ]
    df = spark.createDataFrame(data, schema=schema)
    df.coalesce(1)\
        .write\
        .parquet(path=fs + output,
                 mode="overwrite")


def car_colors():
    output = "/data/dims/car_colors"
    schema = T.StructType([
        T.StructField("color_id", T.IntegerType(), False),
        T.StructField("color_name", T.StringType(), True)
    ])
    data = [
        [1, "Black"],
        [2, "Red"],
        [3, "Gray"],
        [4, "White"],
        [5, "Green"],
        [6, "Blue"],
        [7, "Pink"]
    ]
    df = spark.createDataFrame(data, schema=schema)
    df.coalesce(1) \
        .write \
        .parquet(path=fs + output,
                 mode="overwrite")


if __name__ == "__main__":
    try:
        car_models()
        car_colors()
    finally:
        spark.stop()
