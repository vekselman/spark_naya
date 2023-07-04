from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.master('local[*]').appName('example_app').getOrCreate()

example_data = [Row('one'), Row('two'), Row('three'), Row('four')]
cols = ['my_col']

data_df = spark.createDataFrame(data=example_data, schema=cols)

data_df.show()

spark.stop()
