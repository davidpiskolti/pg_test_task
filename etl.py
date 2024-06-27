from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, weekofyear

# Initialize Spark session
spark = SparkSession.builder.appName("E-Commerce ETL").getOrCreate()

#1 Load data
orders = spark.read.csv('C:/Users/David_Piskolti/Downloads/pg_task_data/olist_orders_dataset.csv', header=True, inferSchema=True)
order_items = spark.read.csv('C:/Users/David_Piskolti/Downloads/pg_task_data/olist_order_items_dataset.csv', header=True, inferSchema=True)
products = spark.read.csv('C:/Users/David_Piskolti/Downloads/pg_task_data/olist_products_dataset.csv', header=True, inferSchema=True)

#2 Data Cleaning and preprocessing
# Convert dates to datetime objects
orders = orders.withColumn('order_purchase_timestamp', to_date(col('order_purchase_timestamp')))

# Select relevant columns
order_items = order_items.select('order_id', 'product_id', 'price')
orders = orders.select('order_id', 'order_purchase_timestamp')
products = products.select('product_id', 'product_category_name')

# Merge datasets
merged_df = order_items.join(orders, on='order_id').join(products, on='product_id')

# Add 'week' column for grouping by week
merged_df = merged_df.withColumn('week', weekofyear(col('order_purchase_timestamp')))

# Group by product and week to aggregate sales
weekly_sales = merged_df.groupBy('product_id', 'week').sum('price').withColumnRenamed('sum(price)', 'total_sales')