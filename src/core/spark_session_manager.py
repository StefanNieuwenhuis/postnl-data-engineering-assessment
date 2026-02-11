import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from core.configuration_manager import ConfigurationManager
from utils.logging_utils import log_footer, log_header

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Manages SparkSession Lifecycle"""

    def __init__(self, cm: ConfigurationManager) -> None:
        """
        Build and store a SparkSession from the given configuration.

        :param cm: ConfigurationManager instance (environment and spark.* settings)
        """
        self.cm = cm
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """
        Create SparkSession from config

        :return: Configured SparkSession
        """
        environment = self.cm.get_environment()
        app_name = self.cm.get("spark", environment, "app_name")
        builder = SparkSession.builder.appName(app_name)

        # Set master
        master = self.cm.get("spark", "local", "master", default="local[*]")
        builder = builder.master(master)

        # Apply environment-specific configurations
        spark_config = self.cm.get("spark", environment, "config", default={})
        for key, value in spark_config.items():
            builder = builder.config(key, value)

        extra_packages = ["org.apache.hadoop:hadoop-aws:3.3.4"]

        spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=extra_packages,
        ).getOrCreate()

        log_header(f"Created SparkSession with Master: {master} for {environment} environment")
        logger.info(f"Master: {master}")
        logger.info(f"Environment: {environment}")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
        log_footer()

        return spark

    def get_session(self) -> SparkSession:
        """
        Return the active SparkSession.

        :return: The SparkSession created at init.
        """
        return self.spark

    def stop(self) -> None:
        """
        Stop the SparkSession when not on Databricks.

        No-op on Databricks; stops the session locally to release resources.
        """
        if self.cm.get_environment() != "databricks":
            self.spark.stop()
            log_header("SparkSession stopped and its resources released")
