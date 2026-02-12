import logging
from typing import Optional, List

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

class DeltaSink:
    """Generic Delta Sink to write to Delta tables"""

    @staticmethod
    def upsert_with_merge(spark: SparkSession, df: DataFrame, output_path: str, dataset_name: str, merge_keys: Optional[List[str]] = None) -> None:
        if merge_keys and DeltaTable.isDeltaTable(spark, output_path):
            merge_condition = " AND ".join(
                f"target.{k} = source.{k}" for k in merge_keys
            )
            delta_table = DeltaTable.forPath(spark, output_path)

            (delta_table.alias("target")
             .merge(df.alias("source"), merge_condition)
             .whenNotMatchedInsertAll()
             .execute())

            logger.info(f"Upsert merge {dataset_name} with keys: {merge_keys} complete")
        else:
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("ingestion_date")
                .save(output_path)
            )

            logger.info(f"Appended {dataset_name} to data layer")
