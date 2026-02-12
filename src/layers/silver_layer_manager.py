import logging
from typing import List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import StructType

from core.configuration_manager import ConfigurationManager
from sinks.delta_sink import DeltaSink
from utils.quarantine_utils import QuarantineUtils

logger = logging.getLogger(__name__)

QUARANTINE_REASON_COLUMN = "_quarantine_reason"


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

        logger.info("Start normalizing date/timestamp columns")
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

        logger.info("Normalizing date/timestamp columns complete")
        return df

    def _deduplicate(self, df: DataFrame, key_cols: Optional[List[str]] = None) -> Tuple[DataFrame, DataFrame]:
        """
        Deduplicate DataFrame by configured deduplication keys

        :param df: Input DataFrame
        :param key_cols: deduplication column keys
        :return: Tuple of valid and duplicate DataFrames
        """
        if not key_cols:
            # no deduplication keys available; return unaltered
            empty_df = self.spark.createDataFrame(data = [], schema= StructType([]))
            return df, empty_df

        logger.info("Start deduplicating records by keys")
        window = Window.partitionBy(*key_cols).orderBy(F.desc("ingestion_timestamp"))
        df_ranked = df.withColumn("_rn", F.row_number().over(window))

        df_kept = df_ranked.filter("_rn = 1").drop("_rn")
        df_dropped = (
            df_ranked.filter("_rn > 1")
            .drop("_rn")
            .withColumn("drop_reason", F.lit("duplicate_key"))
        )

        logger.info(f"Deduplication complete")

        return df_kept, df_dropped



    def _handle_missing_values(
        self, df: DataFrame, key_cols: Optional[List[str]] = None
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Handles missing values in key columns (drops rows with nulls).
        For quarantine path, use _split_valid_invalid instead.
        """
        if not key_cols:
            empty_df = self.spark.createDataFrame(data=[], schema=StructType([]))
            return df, empty_df

        logger.info("Start handling missing values")

        cond = None
        for c in key_cols:
            expr = F.col(c).isNull()
            cond = expr if cond is None else (cond | expr)

        classified = df.withColumn("_reason", F.when(cond, "missing_required"))

        invalid_df = classified.filter("_reason IS NOT NULL")
        valid_df = classified.filter("_reason IS NULL").drop("_reason")

        logger.info("Missing values handled")

        return valid_df, invalid_df


    def transform_all(self) -> None:
        datasets = self.cm.get_datasets()

        for dataset_name in datasets:
            logger.info("=" * 80)
            logger.info(f"Start transforming {dataset_name}".center(80))
            logger.info("=" * 80)

            # define source/target/checkpoint paths
            source_layer = "bronze"
            target_layer = "silver"
            data_source_path = self.cm.get_layer_path(source_layer, dataset_name)
            target_path = self.cm.get_layer_path(target_layer, dataset_name)
            checkpoint_path = f"{self.cm.get_bucket(target_layer)}/checkpoints/{dataset_name}"

            # Fetch transformation configuration keys
            transformation_cfg = self.cm.get_transformations(target_layer, dataset_name) or {}
            dedupe_keys = transformation_cfg.get("dedupe_keys") or []
            required_cols = transformation_cfg.get("required_columns") or []
            merge_keys = self.cm.get_merge_keys(dataset_name)

            def process_batch(batch_df: DataFrame, batch_id) -> None:
                logger.info(f"Merge upsert batch {batch_id} to Delta Sink")

                if batch_df.isEmpty():
                    return

                # deduplication
                duplicate_dataset_name = f"duplicate_{dataset_name}"
                df_valid_deduplicates, df_duplicates = self._deduplicate(batch_df, dedupe_keys)

                # Handle missing values
                missing_dataset_name = f"missing_required_{dataset_name}"
                df_valid_clean, df_missing = self._handle_missing_values(df_valid_deduplicates, required_cols)

                final_df = self._normalize_timestamps(df_valid_clean, dataset_name)

                DeltaSink.upsert_with_merge(self.spark, final_df, target_path, dataset_name, merge_keys)

                # Quarantine duplicates and missing values
                QuarantineUtils.merge_upsert(self.spark, df_duplicates, self.cm.get_quarantine_path(duplicate_dataset_name, target_layer), duplicate_dataset_name)
                QuarantineUtils.merge_upsert(self.spark, df_missing, self.cm.get_quarantine_path(missing_dataset_name, target_layer),missing_dataset_name)

            # stream data source, and apply transformations
            stream_df = (
                self.spark.readStream
                .load(path=data_source_path, format="delta")
            )

            # merge upsert
            query = (
                stream_df.writeStream
                .foreachBatch(process_batch)
                .option("checkpointLocation", checkpoint_path)
                .trigger(availableNow=True)
                .start()
            )
            query.awaitTermination()

            logger.info("=" * 80)
            logger.info(f"Finished transforming {dataset_name}".center(80))
            logger.info("=" * 80)



