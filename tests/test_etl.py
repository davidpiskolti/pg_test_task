import pytest
from pyspark.sql import SparkSession
from etl import ETL
from config import Config

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("E-Commerce ETL Test").master("local[2]").getOrCreate()

@pytest.fixture
def config():
    return Config('config/brazil.yml')

@pytest.fixture
def etl(config):
    return ETL(config)

def test_load_data(etl):
    orders, order_items, products = etl.load_data()
    assert orders.count() > 0
    assert order_items.count() > 0
    assert products.count() > 0

def test_preprocess_data(etl):
    orders, order_items, products = etl.load_data()
    orders, order_items, products = etl.preprocess_data(orders, order_items, products)
    assert 'order_purchase_timestamp' in orders.columns
    assert 'order_id' in order_items.columns
    assert 'product_id' in products.columns

def test_merge_data(etl):
    orders, order_items, products = etl.load_data()
    orders, order_items, products = etl.preprocess_data(orders, order_items, products)
    merged_df = etl.merge_data(orders, order_items, products)
    assert merged_df.count() > 0

def test_aggregate_data(etl):
    orders, order_items, products = etl.load_data()
    orders, order_items, products = etl.preprocess_data(orders, order_items, products)
    merged_df = etl.merge_data(orders, order_items, products)
    weekly_sales = etl.aggregate_data(merged_df)
    assert weekly_sales.count() > 0

def test_sample():
    assert True
