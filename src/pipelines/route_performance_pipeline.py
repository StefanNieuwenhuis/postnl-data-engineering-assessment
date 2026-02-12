import logging
from datetime import datetime

from pyspark.sql import SparkSession

from core.configuration_manager import ConfigurationManager
from layers.bronze_layer_manager import BronzeLayerManager
from layers.gold_layer_manager import GoldLayerManager
from layers.silver_layer_manager import SilverLayerManager

logger = logging.getLogger(__name__)


class RoutePerformancePipeline:
    """
    Pipeline that runs ingestion/transformation/aggregation
    """

    def _generate_run_id(self) -> str:
        """
        Generate a unique run id based on timestamp

        :return: Unique pipeline run ID
        """
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self, spark: SparkSession, cm: ConfigurationManager) -> None:
        self.cm = cm
        self.spark = spark
        self.run_id = self._generate_run_id()

    def run(self) -> None:
        logger.info("=" * 80)
        logger.info("Start Route performance pipeline".center(80))
        logger.info("=" * 80)

        bronze_manager = BronzeLayerManager(self.spark, self.cm)
        silver_manager = SilverLayerManager(self.spark, self.cm)
        gold_manager = GoldLayerManager(self.spark, self.cm)

        bronze_manager.ingest_all(self.run_id)
        silver_manager.transform_all()
        gold_manager.compute_kpis()

        logger.info("=" * 80)
        logger.info("Finished Route performance pipeline".center(80))
        logger.info("=" * 80)
