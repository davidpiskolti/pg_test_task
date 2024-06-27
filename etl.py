from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, weekofyear

# Initialize Spark session
spark = SparkSession.builder.appName("E-Commerce ETL").getOrCreate()

#1 Load data
orders = spark.read.csv('C:/Users/David_Piskolti/Downloads/pg_task_data/olist_orders_dataset.csv', header=True, inferSchema=True)
order_items = spark.read.csv('C:/Users/David_Piskolti/Downloads/pg_task_data/olist_order_items_dataset.csv', header=True, inferSchema=True)
products = spark.read.csv('C:/Users/David_Piskolti/Downloads/pg_task_data/olist_products_dataset.csv', header=True, inferSchema=True)
