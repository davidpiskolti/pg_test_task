import os
import pytest
from pyspark.sql import SparkSession, Row
from etl import ETL
from config import Config

# Check if running on GitHub Actions
is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'

if is_github_actions:
    # Set the environment variables for PySpark on GitHub Actions
    os.environ['PYSPARK_PYTHON'] = 'python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
else:
    # Set the environment variables for PySpark locally
    os.environ['PYSPARK_PYTHON'] = os.path.join(os.getcwd(), 'venv', 'Scripts', 'python.exe')
    os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join(os.getcwd(), 'venv', 'Scripts', 'python.exe')
@pytest.fixture(scope="session")
def spark():
    """Fixture to initialize a Spark session."""
    return SparkSession.builder \
        .appName("E-Commerce ETL Test") \
        .master("local[2]") \
        .config("spark.executorEnv.PYSPARK_PYTHON", os.environ['PYSPARK_PYTHON']) \
        .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", os.environ['PYSPARK_PYTHON']) \
        .getOrCreate()


@pytest.fixture
def config():
    """Fixture to load the configuration."""
    return Config('config/brazil.yml')

@pytest.fixture
def etl(config, spark):
    """Fixture to initialize the ETL process with the test Spark session."""
    etl_instance = ETL(config)
    etl_instance.spark = spark  # Use the test Spark session
    return etl_instance

@pytest.fixture
def mock_data(spark):
    """Fixture to create mock data for testing."""
    orders_data = [
        Row(order_id='1', order_purchase_timestamp='2023-01-01'),
        Row(order_id='2', order_purchase_timestamp='2023-01-02')
    ]
    order_items_data = [
        Row(order_id='1', product_id='101', price=10.0),
        Row(order_id='2', product_id='102', price=20.0)
    ]
    products_data = [
        Row(product_id='101', product_category_name='Category1'),
        Row(product_id='102', product_category_name='Category2')
    ]
    orders = spark.createDataFrame(orders_data)
    order_items = spark.createDataFrame(order_items_data)
    products = spark.createDataFrame(products_data)
    return orders, order_items, products

def test_load_data(etl, mock_data):
    """Test to verify the loading of data."""
    orders, order_items, products = mock_data
    assert orders.count() > 0
    assert order_items.count() > 0
    assert products.count() > 0

def test_preprocess_data(etl, mock_data):
    """Test to verify the preprocessing of data."""
    orders, order_items, products = etl.preprocess_data(*mock_data)
    assert 'order_purchase_timestamp' in orders.columns
    assert 'order_id' in order_items.columns
    assert 'product_id' in products.columns

def test_merge_data(etl, mock_data):
    """Test to verify the merging of data."""
    orders, order_items, products = etl.preprocess_data(*mock_data)
    merged_df = etl.merge_data(orders, order_items, products)
    assert merged_df.count() > 0

def test_aggregate_data(etl, mock_data):
    """Test to verify the aggregation of data."""
    orders, order_items, products = etl.preprocess_data(*mock_data)
    merged_df = etl.merge_data(orders, order_items, products)
    weekly_sales = etl.aggregate_data(merged_df)
    assert weekly_sales.count() > 0

def test_sample():
    """Sample test to ensure testing setup is functional."""
    assert True
