from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("read_data") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("Analysis of retail sales by Pyspark")
print("=" * 60)

import pandas as pd

print("Reading data")
pdf = pd.read_excel("./data/Online Retail.xlsx")
df = spark.createDataFrame(pdf)

print(f"Number of rows: {df.count()}")
print(f"Number of columns: {len(df.columns)}")
print(f"Columns name: {df.columns}")
print("Head of DataFrame")
print(df.head())
print("Data Types of data")
print(df.printSchema())
spark.stop()
