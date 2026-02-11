import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from core.configuration_manager import ConfigurationManager

logger = logging.getLogger(__name__)

class GoldLayerManager:
    def __init__(self, spark: SparkSession, cm: ConfigurationManager):
        self.spark = spark
        self.cm = cm

    def _compute_delay_minutes(self, df: DataFrame) -> DataFrame:
        """
        Compute the delay in minutes
        When the delay is negative, the shipment arrived early

        :param df: Input DataFrame
        :return: DataFrame with added columns `delay_minutes` & `arrival_status`
        """
        return (
            df
            .withColumn("delay_minutes",
                 F.round(
                     (F.col("actual_arrival").cast("long") - F.col("planned_arrival").cast("long")) / 60,
                     2
                 )
            )
            .withColumn("arrival_status",
                F.when(F.col("delay_minutes") > 0, "LATE")
                        .when(F.col("delay_minutes") < 0, "EARLY")
                        .when(F.col("delay_minutes").isNull(), "NULL") # TODO: Handle missing values for NULL ts
                        .otherwise("ON_TIME")
            )
        )

    def compute_kpis(self) -> None:
        source_layer = "silver"
        dest_layer = "gold"
        output_format = "delta"

        shipments_source_path = self.cm.get_layer_path(source_layer, "shipments")
        shipments_df = self.spark.read.format(output_format).load(shipments_source_path)

        vehicles_source_path = self.cm.get_layer_path(source_layer, "vehicles")
        vehicles_df = self.spark.read.format(output_format).load(vehicles_source_path)

        logger.info(f"shipments columns: {shipments_df.columns}")
        logger.info(f"vehicles columns: {vehicles_df.columns}")

        query = (
            self.spark.read.format(output_format).load(shipments_source_path)
            .transform(lambda df: self._compute_delay_minutes(df))
        )

        query.select(["shipment_id", "planned_arrival", "actual_arrival", "delay_minutes", "arrival_status"]).show()