import logging
from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable

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

    def _upsert_with_merge(self,
                           df: DataFrame,
                           output_path: str,
                           dataset_name: str,
                           merge_keys: Optional[List[str]] = None
                           ) -> None:
        if merge_keys and DeltaTable.isDeltaTable(self.spark, output_path):
            merge_condition = " AND ".join(
                f"target.{k} = source.{k}" for k in merge_keys
            )
            delta_table = DeltaTable.forPath(self.spark, output_path)

            (delta_table.alias("target")
             .merge(df.alias("source"), merge_condition)
             .whenNotMatchedInsertAll()
             .execute())
            logger.info(f"Merged {dataset_name} into silver (keys: {merge_keys})")
        else:
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("ingestion_date")
                .save(output_path)
            )
            logger.info(f"Appended {dataset_name} to bronze")


    def transform_all(self) -> None:
        source_layer = "bronze"
        dest_layer = "silver"

        datasets = self.cm.get_datasets()
        logger.info(datasets)

        for dataset_name in datasets:
            data_source_path = self.cm.get_layer_path(source_layer, dataset_name)
            output_path = self.cm.get_layer_path(dest_layer, dataset_name)
            checkpoint_path = f"{self.cm.get_bucket('silver')}/checkpoints/{dataset_name}"

            transformation_cfg = self.cm.get_transformations(dest_layer, dataset_name) or {}

            dedupe_keys = transformation_cfg.get("dedupe_keys") or []
            required_cols = transformation_cfg.get("required_columns") or []

            merge_keys = self.cm.get_merge_keys(dataset_name)

            logger.info(f"Deduplication keys: {dedupe_keys}")
            logger.info(f"Required column keys: {required_cols}")
            logger.info(f"Merge keys: {merge_keys}")

            logger.info("=" * 80)
            logger.info(f"Start transforming {dataset_name}".center(80))
            logger.info("=" * 80)

            stream_df = (
                read_stream(self.spark, data_source_path)
                .transform(lambda df: self._deduplicate(df, dedupe_keys))
                .transform(lambda df: self._handle_missing_values(df, required_cols))
                .transform(lambda df: self._normalize_timestamps(df, dataset_name))
            )

            def merge_batch(batch_df: DataFrame, batch_id: int) -> None:
                """Merge each micro-batch into silver for idempotency"""
                logger.info(f"Process micro batch #{batch_id}")

                if batch_df.isEmpty():
                    return
                self._upsert_with_merge(batch_df, output_path, dataset_name, merge_keys)

            query = (
                stream_df.writeStream
                .foreachBatch(merge_batch)
                .option("checkpointLocation", checkpoint_path)
                .trigger(availableNow=True)
                .start()
            )

            query.awaitTermination()

            logger.info("=" * 80)
            logger.info(f"Finished transforming {dataset_name}".center(80))
            logger.info("=" * 80)
