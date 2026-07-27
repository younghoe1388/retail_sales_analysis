# 🛒 E-commerce Data Analysis (PySpark + Docker)

This is an e-commerce data analysis project based on PySpark, including data cleaning, multidimensional analysis, and visualization, running in a Docker container.

## 📊 Analysis Content

- Monthly Sales Trends

- Sales Ranking by Country

- Top 10 Best-Selling Products

- Return Rate Analysis

- Weekend vs. Weekday Sales Comparison

- Customer Value Segmentation

- Data visualization

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

## Visualization result (Matplotlib & Seaborn)
<img width="1800" height="900" alt="image" src="https://github.com/user-attachments/assets/5d89305d-15cf-4d68-95e6-673a9a58fdee" />
<img width="1800" height="900" alt="image" src="https://github.com/user-attachments/assets/8b6c3bba-3b8e-4df4-b315-62ba5efce5e8" />
<img width="1800" height="900" alt="image" src="https://github.com/user-attachments/assets/b7436b67-5d8f-4ca3-a27e-e18900b5a8e7" />




- Docker

## 📁 Data Source

[Online Retail Data] Set](https://archive.ics.uci.edu/ml/datasets/Online+Retail)
