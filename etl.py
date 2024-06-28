import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, weekofyear, sum as spark_sum
from config import Config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class ETL:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.appName("E-Commerce ETL").getOrCreate()
    def load_data(self):
        logger.info("Loading data...")
        orders = self.spark.read.csv(self.config.input_paths['orders'], header=True, inferSchema=True)
        order_items = self.spark.read.csv(self.config.input_paths['order_items'], header=True, inferSchema=True)
        products = self.spark.read.csv(self.config.input_paths['products'], header=True, inferSchema=True)
        return orders, order_items, products
    def preprocess_data(self, orders, order_items, products):
        logger.info("Preprocessing data...")
        orders = orders.withColumn('order_purchase_timestamp', to_date(col(self.config.columns['orders']['order_purchase_timestamp'])))
        order_items = order_items.select(self.config.columns['order_items']['order_id'], self.config.columns['order_items']['product_id'], self.config.columns['order_items']['price'])
        orders = orders.select(self.config.columns['orders']['order_id'], self.config.columns['orders']['order_purchase_timestamp'])
        products = products.select(self.config.columns['products']['product_id'], self.config.columns['products']['product_category_name'])
        return orders, order_items, products
    def merge_data(self, orders, order_items, products):
        logger.info("Merging data...")
        merged_df = order_items.join(orders, on=self.config.columns['order_items']['order_id']).join(products, on=self.config.columns['order_items']['product_id'])
        return merged_df

    def aggregate_data(self, merged_df):
        logger.info("Aggregating data...")
        merged_df = merged_df.withColumn('week', weekofyear(col(self.config.columns['orders']['order_purchase_timestamp'])))
        weekly_sales = merged_df.groupBy(self.config.columns['order_items']['product_id'], 'week').agg(
            spark_sum(col(self.config.columns['order_items']['price'])).alias('total_sales')
        )
        return weekly_sales

    def save_data(self, weekly_sales):
        logger.info(f"Saving data to {self.config.output_path}")
        weekly_sales.write.mode('overwrite').partitionBy(self.config.columns['products']['product_id']).parquet(self.config.output_path)

    def run(self):
        try:
            orders, order_items, products = self.load_data()
            orders, order_items, products = self.preprocess_data(orders, order_items, products)
            merged_df = self.merge_data(orders, order_items, products)
            weekly_sales = self.aggregate_data(merged_df)
            self.save_data(weekly_sales)
            logger.info("ETL process completed successfully.")
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    config_path = 'config/brazil.yml'
    config = Config(config_path)
    etl = ETL(config)
    etl.run()
