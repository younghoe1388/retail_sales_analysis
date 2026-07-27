# 🛒 E-commerce Data Analysis (PySpark + Docker)

This is an e-commerce data analysis project based on PySpark, including data cleaning, multidimensional analysis, and visualization, running in a Docker container.

## 📊 Analysis Content

- Monthly Sales Trends

- Sales Ranking by Country

- Top 10 Best-Selling Products

- Return Rate Analysis

- Weekend vs. Weekday Sales Comparison

- Customer Value Segmentation

## 🚀 Quick Start

```bash

# 1. Start the Docker container

docker-compose up -d

# 2. Enter the container

docker exec -it retail_sales_analysis-spark-1 bash

# 3. Run the analysis

/opt/spark/bin/spark-submit /app/analysis.py

# 4. View the charts

ls /app/output/

```

## 🛠 Technology Stack

- PySpark

- Pandas

- Matplotlib / Seaborn

- Docker

## 📁 Data Source

[Online Retail Data] Set](https://archive.ics.uci.edu/ml/datasets/Online+Retail)
