import argparse
import logging
from config import Config
from etl import ETL

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(config_path):
    logger.info(f"Running ETL with configuration: {config_path}")
    config = Config(config_path)
    etl = ETL(config)
    etl.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ETL pipeline for the Brazilian E-Commerce dataset")
    parser.add_argument('config', type=str, help='Path to the configuration file')
    args = parser.parse_args()

    main(args.config)
