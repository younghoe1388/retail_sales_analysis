from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, countDistinct, when, round, desc, year, month, to_date, isnan, isnull, dayofweek
from pyspark.sql.types import StringType
import pandas as pd

spark = SparkSession.builder.appName("read_data") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 75)
print("Analysis report of retail sales by Pyspark")
print("=" * 75)

# 第一步：数据读取
# Read data
print("\nReading data")
pdf = pd.read_excel("/app/data/Online Retail.xlsx")
df = spark.createDataFrame(pdf)

# 第二步：数据清洗
# Data cleaning
# 2.1 缺失值处理
from pyspark.sql.functions import sum as spark_sum
missing_counts = df.select([spark_sum(isnull(col(c)).cast("int")).alias(c) for c in df.columns]).collect()[0]
total_rows = df.count()
for c in df.columns:
    missing = getattr(missing_counts, c)
    if missing > 0:
        ratio = missing / total_rows * 100
        print(f"Missing {missing} data, ratio: {ratio:.2f}%")

# Description 缺失值处理：若一件物品缺失简介 则无法判断其物品为何物 因此删除所有Description为NA的行
before = df.count()
df = df.filter(col("Description").isNotNull())
after = df.count()
print(f"Deleted {before - after} rows")

# CustomerID缺失值处理
# CustomerID缺失量过大 (5% - 20%以上) 不宜删除 因此填充Unknown代替
df = df.fillna({"CustomerID": "Unknown"})
print(f"NA values of CustomerID already filled by Unknown")

# 2.2：异常值处理
# Quantity异常值处理：Quantity不可能小于0
quantity_neg = df.filter(col("Quantity") < 0).count()
quantity_ratio = quantity_neg / df.count() * 100
print(f"Quantity < 0 (return) ratio: {quantity_ratio:.2f}%")

# UnitPrice = 0：保留（赠品），不做处理
up_count = df.filter(col("UnitPrice") == 0).count()
up_ratio = up_count / df.count() * 100
print(f"Numbers of gift: {up_count}, ratio: {up_ratio:.2f}%")

# UnitPrice < 0：删除（只有2条）
up_neg = df.filter(col("UnitPrice") < 0).count()
up_ratio_neg = up_neg / df.count() * 100
print(f"Negative unit price count: {up_neg}, ratio: {up_ratio_neg:.2f}%")

before = df.count()
df = df.filter(col("UnitPrice") >= 0)
after = df.count()
print(f"Totally deleted {before - after} rows")

# UnitPrice > 1000：先看看具体是什么商品
up_high = df.filter(col("UnitPrice") >= 1000).count()
up_high_ratio = up_high / df.count() * 100
print(f"Numbers of high unitprice (> 1000): {up_high}, ratio: {up_high_ratio:.2f}%")

# 2.3 特征分析
print(f"Features construction")

# 增加总金额列
df = df.withColumn("total_amount", col("Quantity") * col("UnitPrice"))

# 转换日期类型
df = df.withColumn("InvoiceDate", to_date(col("InvoiceDate")))

# 标记退货 (Quantity < 0)
df = df.withColumn("is_cancelled", when(col("Quantity") < 0, True).otherwise(False))
cancelled_count = df.filter(col("is_cancelled") == True).count()
print(f"Number of cancelled items: {cancelled_count}")

# 标记取消订单 (InvoiceNo 以 'C' 开头)
df = df.withColumn("cancel", when(col("InvoiceNo").cast(StringType()).startswith("C"), True).otherwise(False))
cancel_count = df.filter(col("cancel") == True).count()
print(f"Number of cancelled items: {cancel_count}")

# 计算周末标识
df = df.withColumn("is_weekend", when(dayofweek("InvoiceDate").isin([1, 7]), True).otherwise(False))
weekend_count = df.filter(col("is_weekend") == True).count()
print(f"Number of weekend record: {weekend_count}")

print(f"Rows of data after cleaning data: {df.count()}")


# 第三步：核心分析
# Core analysis
print("\n" + "=" * 70)
print("Core analysis")
print("=" * 70)

# 3.1 每月销售额趋势
print("\nTrend of retail sales monthly")
monthly = df.filter(col("is_cancelled") == False) \
    .withColumn("year", year(col("InvoiceDate"))) \
    .withColumn("month", month(col("InvoiceDate"))) \
    .groupBy("year", "month") \
    .agg(round(sum("total_amount"), 2).alias("total_sales"), countDistinct("InvoiceNo").alias("order_count")) \
    .orderBy(desc("total_sales")) \
    .limit(12)
monthly.show(12, truncate=False)

# 3.2 各国销售额排行
print(f"Ranking of sales between countries")
country_sales = df.filter(col("is_cancelled") == False) \
    .groupBy("Country") \
    .agg(round(sum("total_amount"), 2).alias("total_sales"), countDistinct("InvoiceNo").alias("order_count")) \
    .orderBy(desc("total_sales")) .limit(12)
country_sales.show(12, truncate=False)

# 3.3 畅销商品 TOP 10
print(f"\nTop 10 sales of item")
top_sales = df.filter(col("is_cancelled") == False) \
    .groupBy("StockCode", "Description") \
    .agg(sum("Quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity")) .limit(10)
top_sales.show(10, truncate=False)

# 3.4 退货率分析
print(f"\nReturning rate analysis")
orders = df.select("InvoiceNo", "is_cancelled").distinct()
total_orders = orders.count()
return_orders = orders.filter(col("is_cancelled") == True).count()
print(f"Rate of returning: {return_orders / total_orders * 100:.2f}%")

# 3.5 周末 vs 工作日
print(f"Sales of weekend vs weekdays")
weekend_sales = df.filter(col("is_cancelled") == False) \
    .groupBy("is_weekend")\
    .agg(round(sum("total_amount"), 2).alias("total_sales"), countDistinct("InvoiceNo").alias("order_count"))\
    .orderBy(desc("total_sales")) .limit(12)
weekend_sales.show(2, truncate=False)

# 3.6 客户价值分层 (TOP 10)
print("\nTop 10 customer")
customer_sales = df.filter(col("is_cancelled") == False)\
    .groupBy("CustomerID") \
    .agg(round(sum("total_amount"), 2).alias("total_spent"), countDistinct("InvoiceNo").alias("order_count"))\
    .orderBy(desc("total_spent")) .limit(12)

customer_sales.show(12, truncate=False)

print("\n" + "=" * 70)
print("All analysis are completed")
print("=" * 70)

# 第四步：可视化
# Visualisation
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
print("\n" + "=" * 70)
print(f"Visualizing sales data")
print("=" * 70)

import os
os.makedirs("/app/output", exist_ok=True)

# 4.1 每月销售额趋势
monthly_pd = monthly.toPandas()
monthly_pd['date'] = monthly_pd.apply(lambda row: f"{row['year']}-{int(row['month']):02d}", axis=1)
plt.figure(figsize=(12, 6))
sns.barplot(x = "date", y = "total_sales", data = monthly_pd)
plt.title("Monthly sales trend", fontsize = 16)
plt.xlabel("Date")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/app/output/monthly_sales.png', dpi=150)
print(f"Saving monthly sales plot")

# 4.2 各国销售额 TOP12
country_pd = country_sales.toPandas()
plt.figure(figsize=(12, 6))
sns.barplot(x = "Country", y = "total_sales", data = country_pd.head(12))
plt.title("Top 12 Countries by Sales", fontsize = 16)
plt.xlabel("Country")
plt.ylabel("Total Sales")
plt.tight_layout()
plt.savefig('/app/output/country_sales.png', dpi=150)
print("Saving top 12 countries sales")

# 4.3 畅销商品 TOP10
product_pd = top_sales.toPandas()
plt.figure(figsize=(12, 6))
sns.barplot(x = "Description", y = "total_quantity", data = product_pd.head(10))
plt.title("Top 10 Products by Sales", fontsize = 16)
plt.xlabel("Product")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/app/output/top_products.png', dpi=150)
print(f"Saving top 10 products sales")


spark.stop()