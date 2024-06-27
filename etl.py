from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, weekofyear
from config import Config
class ETL:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.appName("E-Commerce ETL").getOrCreate()
    def load_data(self):
        self.orders = self.spark.read.csv(self.config.input_paths['orders'], header=True, inferSchema=True)
        self.order_items = self.spark.read.csv(self.config.input_paths['order_items'], header=True, inferSchema=True)
        self.products = self.spark.read.csv(self.config.input_paths['products'], header=True, inferSchema=True)

    def run(self):
        self.load_data()

config_path = 'config/brazil.yml'
config = Config(config_path)
etl = ETL(config)
etl.run()
#1 Load data
print(etl.orders)
order_items = etl.order_items
orders = etl.orders
products = etl.products
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

#3 save the output as parquet
# Write the output to Parquet, partitioned by product
output_path = "C:/Users/David_Piskolti/Downloads/pg_task_data/output/parquet"
weekly_sales.write.mode('overwrite').partitionBy('product_id').parquet(output_path)
