import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from core.configuration_manager import ConfigurationManager
from schema_registry.schema_registry import (
    CORRUPT_RECORD_COLUMN,
    SCHEMAS,
    schema_with_corrupt_record,
)
from sinks.delta_sink import DeltaSink
from utils.quarantine_utils import QuarantineUtils
from utils.spark_utils import SparkUtils

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

    def _ingest_batch(
        self,
        dataset_name: str,
        source_path: str,
        run_id: str,
        data_format: str,
        schema: Optional[StructType] = None,
        quarantine: bool = False,
    ) -> DataFrame:
        """
        Ingest data from a batch data source
        When provided, the schema is enforced. Otherwise, Spark infers the schema.
        When quarantine=True and schema is used, malformed records are captured to quarantine.

        :param dataset_name: Data source name (e.g. 'shipments', 'vehicles')
        :param source_path: Source file path (landing path from config)
        :param data_format: Data format
        :param schema: Optional PySpark StructType
        :param quarantine: If True, use columnNameOfCorruptRecord and write corrupt rows to quarantine
        :return: DataFrame with ingested data and bronze metadata columns.
        """

        logger.info(f"Batch ingesting {dataset_name} from: {source_path}")
        reader = self.spark.read

        if data_format == "csv":
            reader = reader.option("header", "true")

        schema = schema or SCHEMAS.get(dataset_name)
        use_corrupt_record = quarantine and schema is not None

        if schema is not None:
            if use_corrupt_record:
                schema = schema_with_corrupt_record(schema)
                reader = reader.option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
            reader = reader.schema(schema)
            logger.info("Schema applied from registry")
        else:
            reader = reader.option("inferSchema", "true")
            logger.info("Schema inferred by Spark")

        df = reader.load(source_path, format=data_format)

        if use_corrupt_record and CORRUPT_RECORD_COLUMN in df.columns:
            valid_df, corrupt_df = QuarantineUtils.split_valid_invalid(df)
            quarantine_path = self.cm.get_quarantine_path(dataset_name)
            corrupt_df = SparkUtils.add_metadata(
                corrupt_df, run_id, dataset_name, source_file=source_path
            )
            QuarantineUtils.merge_upsert(self.spark, corrupt_df, quarantine_path, dataset_name)

            df = valid_df

        # Add metadata
        df = SparkUtils.add_metadata(df, run_id, dataset_name, source_file=source_path)

        # Write to Bronze layer
        target_path = self.cm.get_layer_path("bronze", dataset_name)
        merge_keys = self.cm.get_merge_keys(dataset_name)

        DeltaSink.upsert_with_merge(self.spark, df, target_path, dataset_name, merge_keys)

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
        logger.info(f"Streaming ingest {dataset_name} from directory: {path_dir}")

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
        stream_with_meta = SparkUtils.add_metadata(
            stream_df, run_id, dataset_name, source_file=path_dir
        )

        checkpoint_path = f"{self.cm.get_bucket('bronze')}/checkpoints/{dataset_name}"
        output_path = self.cm.get_layer_path("bronze", dataset_name)
        merge_keys = self.cm.get_merge_keys(dataset_name)

        def process_batch(batch_df: DataFrame, batch_id: int) -> None:
            """Merge each micro-batch into bronze"""
            if batch_df.isEmpty():
                return
            DeltaSink.upsert_with_merge(self.spark, batch_df, output_path, dataset_name, merge_keys)

        query = (
            stream_with_meta.writeStream.foreachBatch(process_batch)
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint_path)
            .start()
        )

        query.awaitTermination()

        logger.info(f"Stream ingestion completed for {dataset_name}")

    def ingest_all(self, run_id: str) -> None:
        """
        Execute data ingestion for all configured datasets
        :param run_id:
        :return:
        """

        landing_bucket = self.cm.get_bucket("landing")
        logger.info(f"Start ingesting all configured datasets from {landing_bucket}")

        datasets = self.cm.get_datasets()

        for name, config in datasets.items():
            path = self.cm.get_layer_path("landing", name)
            config = config or {}
            is_stream = config.get("stream") is True
            data_format = config.get("format")

            if is_stream:
                logger.info(f"Start streaming ingest {name} from path: {path}")
                self._ingest_stream(name, path, run_id, data_format)
            else:
                quarantine = config.get("quarantine") is True
                logger.info(f"Start batch ingesting {name} from path: {path}")

                self._ingest_batch(name, path, run_id, data_format, quarantine=quarantine)
