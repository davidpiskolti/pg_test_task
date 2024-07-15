# E-Commerce ETL Pipeline

This project implements an ETL pipeline for the Brazilian E-Commerce dataset using PySpark. The pipeline reads data from CSV files, processes and aggregates it, and saves the output as a Parquet file.

## Prerequisites

Before running the ETL pipeline, ensure you have the following software installed:

- Python 3.11
- Java Development Kit (JDK) 8 or higher

### Installing Dependencies

Install the required Python packages using pip:


pip install pyspark pytest pyyaml

## Project Structure
```
pg_test_task/
│
├── config/
│   └── brazil.yml
│
├── tests/
│   └── test_etl.py
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── etl.py
├── config.py
├── main.py
├── requirements.txt
└── README.md
```

- `etl.py`: Contains the ETL class and its methods.
- `test_etl.py`: Contains unit tests for the ETL pipeline.
- `config.py`: Contains the Config class for loading configurations.
- `main.py`: Main script to run the ETL pipeline.
- `config/brazil.yml`: Configuration file for the Brazilian E-Commerce dataset.
- `.github/workflows/ci.yml`: GitHub Actions configuration for CI/CD.

## Configuration

The configuration file `config/brazil.yml` contains paths to the input datasets, output path, and column mappings. Here's an example configuration:

```yaml
input_paths:
  orders: 'data/olist_orders_dataset.csv'
  order_items: 'data/olist_order_items_dataset.csv'
  products: 'data/olist_products_dataset.csv'
output_path: 'data/output/parquet'
columns:
  orders:
    order_id: 'order_id'
    order_purchase_timestamp: 'order_purchase_timestamp'
  order_items:
    order_id: 'order_id'
    product_id: 'product_id'
    price: 'price'
  products:
    product_id: 'product_id'
    product_category_name: 'product_category_name'
aggregations:
  group_by: ['product_id', 'week']
  aggregate_column: 'price'
  aggregation_function: 'sum'
```
## Running the ETL Pipeline
To run the ETL pipeline, execute the main.py script with the path to the configuration file:
```
python main.py config/brazil.yml
```
## Running Tests
To run the unit tests, use the following command:
```
pytest -v tests/
```
## Continuous Integration
The project includes a GitHub Actions workflow configuration in .github/workflows/ci.yml to run the tests on every pull request. Here is the configuration:
```
name: CI

on: [pull_request]

jobs:
  test:
    runs-on: windows-latest

    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.11'
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
    - name: Run tests
      env:
        PYTHONPATH: ${{ github.workspace }}
      run: |
        pytest -v tests/
```
