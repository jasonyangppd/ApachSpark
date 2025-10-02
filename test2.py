import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLExampleApp").getOrCreate()

csv_file_path = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(csv_file_path)

df.show(5)
