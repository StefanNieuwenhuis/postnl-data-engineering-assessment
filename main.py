import logging

from core.configuration_manager import ConfigurationManager
from core.spark_session_manager import SparkSessionManager
from pipelines.route_performance_pipeline import RoutePerformancePipeline

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def main():


    cm = ConfigurationManager()
    spark_manager = SparkSessionManager(cm)
    spark = spark_manager.get_session()
    pipeline = RoutePerformancePipeline(spark, cm)

    try:
        pipeline.run()
    finally:
        spark_manager.stop()


if __name__ == "__main__":
    main()
