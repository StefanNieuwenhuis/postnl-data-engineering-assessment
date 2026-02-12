import logging
from typing import Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from schema_registry.schema_registry import CORRUPT_RECORD_COLUMN
from sinks.delta_sink import DeltaSink

logger = logging.getLogger(__name__)


class QuarantineUtils:
    @staticmethod
    def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Split a DataFrame in corrupt and valid record DataFrames

        :param df: Input DataFrame
        :return: Tuple of valid and corrupt DataFrames
        """
        corrupt_df = df.filter(F.col(CORRUPT_RECORD_COLUMN).isNotNull())
        valid_df = df.filter(F.col(CORRUPT_RECORD_COLUMN).isNull()).drop(CORRUPT_RECORD_COLUMN)

        return valid_df, corrupt_df

    @staticmethod
    def merge_upsert(
        spark: SparkSession, df: DataFrame, quarantine_path: str, dataset_name: str
    ) -> None:
        """
        Write corrupt records to quarantine Delta table.

        :param spark: Active SparkSession
        :param df: Input DataFrame
        :param dataset_name: Dataset name for quarantine path
        :param quarantine_path: Target path to store the quarantine DataFrame
        """
        if df.isEmpty():
            return

        # Use stable columns for hash (exclude ingestion_timestamp, run_id, etc.) for idempotency
        if CORRUPT_RECORD_COLUMN in df.columns and "source_file" in df.columns:
            hash_input = F.concat_ws("||", F.col("source_file"), F.col(CORRUPT_RECORD_COLUMN))
        else:
            hash_input = F.concat_ws("||", *[F.col(c).cast("string") for c in df.columns])
        quarantine_row_hash = F.sha2(F.coalesce(hash_input, F.lit("")), 256)
        merge_keys = ["source_file", "quarantine_row_hash"]

        # add quarantine row hash as column to the DataFrame
        df_with_hash = df.withColumn("quarantine_row_hash", quarantine_row_hash)

        # upsert (w/ merge) to quarantine
        DeltaSink.upsert_with_merge(spark, df_with_hash, quarantine_path, dataset_name, merge_keys)

        logger.warning(
            f"Quarantined {df_with_hash.count():,} corrupt records for {dataset_name} to {quarantine_path}"
        )
