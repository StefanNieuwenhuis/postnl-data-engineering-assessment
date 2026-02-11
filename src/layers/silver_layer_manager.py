import logging
from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from core.configuration_manager import ConfigurationManager
from utils.spark_utils import read_dataframe, read_stream

logger = logging.getLogger(__name__)


class SilverLayerManager:
    """Handles Silver Layer Data Transformations (e.g. deduplication, casting/normalization, validation & quarantining"""

    def __init__(self, spark: SparkSession, cm: ConfigurationManager):
        self.spark = spark
        self.cm = cm

    def _normalize_timestamps(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Normalize datetime columns

        :param df: Input streaming DataFrame
        :param dataset_name: dataset key to retrieve configurations
        :return: Normalized DataFrame with new date/timestamp columns
        """

        datetime_formats = ["yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd"]
        prefix = "norm_"
        transformations_config = self.cm.get_transformations("silver", dataset_name) or {}

        cols_config = [("date_columns", F.to_date), ("datetime_columns", F.to_timestamp)]

        for raw_key, cast_func in cols_config:
            raw_cols = transformations_config.get(raw_key) or []

            # Apply normalization
            for raw_col in raw_cols:
                df = df.withColumn(
                    f"{prefix}{raw_col}",
                    F.coalesce(*[cast_func(F.col(raw_col), fmt) for fmt in datetime_formats]),
                )

        return df

    def _deduplicate(self, df: DataFrame, key_cols: Optional[List[str]] = None) -> DataFrame:
        """Deduplicate DataFrame by configured deduplication keys"""
        if not key_cols:
            # no deduplication keys available; return all clean & empty duplicates
            return df

        return df.withWatermark("ingestion_timestamp", "10 minutes").dropDuplicates(key_cols)

    def _handle_missing_values(
        self, df: DataFrame, key_cols: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Handles missing values in key columns
        :param df:
        :return:
        """
        if not key_cols:
            return df

        return df.dropna(subset=key_cols)

    def transform_all(self) -> None:
        source_layer = "bronze"
        dest_layer = "silver"
        output_format = "delta"

        datasets = self.cm.get_datasets()
        logger.info(datasets)

        for dataset_name in datasets:
            data_source_path = self.cm.get_layer_path(source_layer, dataset_name)
            output_path = self.cm.get_layer_path(dest_layer, dataset_name)
            checkpoint_path = f"{self.cm.get_bucket('silver')}/checkpoints/{dataset_name}"

            transformation_cfg = self.cm.get_transformations(dest_layer, dataset_name) or {}

            dedupe_keys = transformation_cfg.get("dedupe_keys", {})
            required_cols = transformation_cfg.get("required_columns", {})

            logger.info(f"Deduplication keys: {dedupe_keys}")
            logger.info(f"Required column keys: {required_cols}")

            logger.info("=" * 80)
            logger.info(f"Start transforming {dataset_name}".center(80))
            logger.info("=" * 80)
            query = (
                read_stream(self.spark, data_source_path)
                .transform(lambda df: self._deduplicate(df, dedupe_keys))
                .transform(lambda df: self._handle_missing_values(df, required_cols))
                .transform(lambda df: self._normalize_timestamps(df, dataset_name))
                .writeStream
                .option("checkpointLocation", checkpoint_path)
                .trigger(availableNow=True)
                .start(output_path, format=output_format)
            )
            query.awaitTermination()

            logger.info("=" * 80)
            logger.info(f"Finished transforming {dataset_name}".center(80))
            logger.info("=" * 80)
