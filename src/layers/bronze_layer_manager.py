import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_date, current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType

from core.configuration_manager import ConfigurationManager
from schema_registry.schema_registry import SCHEMAS
from utils.logging_utils import log_footer, log_header

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
        :param source_file: full path of the ingested file or directory (for streaming)
        :return: DataFrame with bronze layer metadata columns
        """
        result = (
            df.withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("run_id", lit(run_id))
            .withColumn("source_system", lit(source))
        )

        # Use input_file_name() only if source_file is not provided (for batch processing)
        # For streaming DataFrames, input_file_name() may not work reliably, so pass source_file
        if source_file is not None:
            result = result.withColumn("source_file", lit(source_file))
            logger.info(f"Source File provided and added")
        else:
            result = result.withColumn("source_file", input_file_name())
            logger.info(f"Source File not provided; inferred input filename")

        return result

    def _ingest_batch(
        self,
        dataset_name: str,
        path: str,
        run_id: str,
        data_format: str,
        schema: Optional[StructType] = None,
    ) -> DataFrame:
        """
        Ingest data from a batch data source
        When provided, the schema is enforced. Otherwise, Spark infers the schema.

        :param dataset_name: Data source name (e.g. 'shipments', 'vehicles')
        :param path: Source file path (landing path from config)
        :param data_format: Data format
        :param schema: Optional PySpark StructType
        :return: DataFrame with ingested data and bronze metadata columns.
        """

        log_header(f"Batch ingesting {dataset_name} from: {path}")
        reader = self.spark.read.option("header", "true")

        if schema is not None:
            reader = reader.schema(schema)
            logger.info("Custom schema applied")
        else:
            reader = reader.option("inferSchema", "true")
            logger.info("Schema inferred by Spark")

        # Create DataFrame from file location
        df = reader.load(path, format=data_format)

        # Add metadata
        df = self._add_metadata(df, run_id, dataset_name, source_file=path)

        # Write to Bronze layer
        output_path = self.cm.get_layer_path("bronze", dataset_name)

        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("ingestion_date")
            .save(output_path)
        )

        logger.info(f"Ingestion complete. Ingested {df.count():,} {dataset_name} records")
        return df

    def _ingest_stream(
        self,
        dataset_name: str,
        path_dir: str,
        run_id: str,
        data_format: str,
    ) -> None:
        """
        Ingest data from a streaming data source

        Uses Spark Structured Streaming with trigger(availableNow=True) to process all files
        in the directory. Each micro-batch is optionally validated and then written.

        :param dataset_name: Data source name (e.g. 'routes', 'weather')
        :param path_dir: Landing directory (e.g. s3a://landing/sources/routes)
        :param run_id: The current pipeline run ID for metadata
        :param data_format: Data format
        """
        log_header(f"Streaming ingest {dataset_name} from directory: {path_dir}")

        # For streaming JSON, a schema has to be inferred first
        if data_format == "json":
            # get schema from registry for dataset
            schema = SCHEMAS.get(dataset_name)

            if schema is None:
                raise ValueError(f"No schema defined for dataset: {dataset_name}")

            logger.info(f"Using predefined schema for {dataset_name}")

            stream_df = (
                self.spark.readStream.schema(schema).option("multiline", "true").json(path_dir)
            )
        else:
            # For other formats, use load() with format
            stream_df = self.spark.readStream.load(path_dir, format=data_format)

        # Add bronze metadata (pass path_dir so source_file is set for streaming)
        stream_with_meta = self._add_metadata(stream_df, run_id, dataset_name, source_file=path_dir)

        checkpoint_path = f"{self.cm.get_bucket('bronze')}/checkpoints/{dataset_name}"
        output_path = self.cm.get_layer_path("bronze", dataset_name)

        # Simple streaming sink: append to Delta, process all available files, then stop
        query = (
            stream_with_meta.writeStream.format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("ingestion_date")
            .start(output_path)
        )
        query.awaitTermination()

        logger.info(f"Stream ingestion completed for {dataset_name}")
        log_footer()

    def ingest_all(self, run_id: str) -> None:
        """
        Execute data ingestion for all configured datasets
        :param run_id:
        :return:
        """

        landing_bucket = self.cm.get_bucket("landing")
        log_header(f"Start ingesting all configured datasets from {landing_bucket}")

        datasets = self.cm.get_datasets()

        for name, config in datasets.items():
            path = self.cm.get_layer_path("landing", name)
            config = config or {}
            is_stream = config.get("stream") is True
            data_format = config.get("data_format")

            if is_stream:
                logger.info(f"Start streaming ingest {name} from path: {path}")
                self._ingest_stream(name, path, run_id, data_format)
            else:
                logger.info(f"Start batch ingesting {name} from path: {path}")
                self._ingest_batch(name, path, run_id, data_format)
