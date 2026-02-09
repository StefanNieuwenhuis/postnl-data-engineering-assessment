import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from core.configuration_manager import ConfigurationManager

logger = logging.getLogger(__name__)


class BronzeLayerManager:
    """Handles Bronze Layer RAW data ingestion"""

    def __init__(self, spark: SparkSession, cm: ConfigurationManager) -> None:
        """
        Initialize Bronze Layer Manager with active SparkSession and ConfigurationManager

        :param spark: Active SparkSession Instance
        :param cm: Current ConfigurationManager Instance
        """
        self.spark = spark
        self.cm = cm

    def _add_metadata(
        self, df: DataFrame, run_id: str, source: str, source_file: Optional[str] = None
    ) -> DataFrame:
        """
        Add bronze layer metadata columns to improve traceability and partitioning

        :param df: DataFrame to add metadata columns
        :param run_id: The current pipeline run ID
        :param source: Source system / dataset name
        :param source_file: full path of the ingested file
        :return: DataFrame with bronze layer metadata columns
        """
        pass

    def _ingest_batch(
        self, name: str, path: str, run_id: str, schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Ingest data from a batch data source
        When provided, the schema is enforced. Otherwise, Spark infers the schema.

        :param name: Data source name (e.g. 'shipments', 'vehicles')
        :param path: Source file path (landing path from config)
        :param run_id: The current pipeline run ID for metadata
        :param schema: Optional PySpark StructType
        :return: DataFrame with ingested data and bronze metadata columns.
        """

        pass

    def _ingest_stream(self, name: str, path_dir: str, run_id: str) -> DataFrame:
        """
        Ingest data from a streaming data source

        :param name: Data source name (e.g. 'shipments', 'vehicles')
        :param path: Source file path (landing path from config)
        :param run_id: The current pipeline run ID for metadata
        :return: DataFrame with ingested data and bronze metadata columns.
        """

    def ingest_all(self, run_id: str) -> None:
        """
        Execute data ingestion for all configured datasets
        :param run_id:
        :return:
        """

        pass

    def read(self, dataset_name: str) -> DataFrame:
        """
        Read a bronze layer table by dataset name

        :param dataset_name: Dataset key in config (e.g. `routes`, `shipments`)
        :return: DataFrame containing the bronze layer table data
        """
        pass

    def read_stream(self, dataset_name: str) -> DataFrame:
        """
        Stream a bronze layer table by dataset name

        :param dataset_name: Dataset key in config (e.g. `routes`, `shipments`)
        :return: DataFrame containing the bronze layer table data
        """
        pass

    def _write(self, df: DataFrame, dataset_name: str) -> None:
        """
        Write DataFrame to bronze layer table
        Appends to the table at the path from config

        :param df: DataFrame
        :param dataset_name: Dataset key in config (e.g. `routes`, `shipments`)
        """

        pass
