import logging

from core.configuration_manager import ConfigurationManager
from core.spark_session_manager import SparkSessionManager
from layers.silver_layer_manager import SilverLayerManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def main():
    cm = ConfigurationManager()
    spark_manager = SparkSessionManager(cm)
    spark = spark_manager.get_session()

    silver_manager = SilverLayerManager(spark, cm)
    silver_manager.transform_all()

if __name__ == "__main__":
    main()
