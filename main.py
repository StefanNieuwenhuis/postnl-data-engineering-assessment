import logging
from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from core.configuration_manager import ConfigurationManager
from core.spark_session_manager import SparkSessionManager
from layers.bronze_layer_manager import BronzeLayerManager
from layers.gold_layer_manager import GoldLayerManager
from layers.silver_layer_manager import SilverLayerManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def _deduplicate(df: DataFrame, column_keys: Optional[List[str]] = None) -> DataFrame:
    if not column_keys:
        return df

    return df.dropDuplicates(column_keys)

def _handle_missing_values(df: DataFrame, column_keys: Optional[List[str]] = None) -> DataFrame:
    if not column_keys:
        return df

    return df.dropna(subset=column_keys)


def _normalize_timestamps(df: DataFrame, date_cols: List[str], ts_cols: List[str]) -> DataFrame:
    datetime_formats = ["yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd"]
    prefix = "norm_"

    for col in date_cols:
        df = df.withColumn(f"{prefix}{col}",
            F.coalesce(*[F.to_date(F.col(col), fmt) for fmt in datetime_formats])
       )
    for col in ts_cols:
        df = df.withColumn(f"{prefix}{col}",
            F.coalesce(*[F.to_timestamp(F.col(col), fmt) for fmt in datetime_formats])
       )
    return df


def main():
    cm = ConfigurationManager()
    spark_manager = SparkSessionManager(cm)
    spark = spark_manager.get_session()

    # bronze_manager = BronzeLayerManager(spark, cm)
    # bronze_manager.ingest_all(datetime.now().strftime("%Y%m%d_%H%M%S"))

    # silver_manager = SilverLayerManager(spark, cm)
    # silver_manager.transform_all()

    # gold_manager = GoldLayerManager(spark, cm)
    # gold_manager.compute_kpis()

    layer = "gold"
    dataset_name = "route_performance"
    data_format = "delta"
    path_dir = f"s3a://{layer}/{dataset_name}"

    df = spark.read.load(path_dir, format=data_format)

    logger.info("")
    logger.info("="*80)
    logger.info(f"Stats for {dataset_name}".center(80))
    logger.info("=" * 80)
    logger.info(f"data source: {path_dir}")
    logger.info(f"record count: {df.count()}")
    logger.info(f"columns: {df.columns}")
    logger.info(f"column types: {df.dtypes}")
    logger.info(f"schema: {df.printSchema()}")
    logger.info("=" * 80)

    df.show(truncate=False)

if __name__ == "__main__":
    main()
