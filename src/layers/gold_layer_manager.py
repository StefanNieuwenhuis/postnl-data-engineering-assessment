import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from core.configuration_manager import ConfigurationManager

logger = logging.getLogger(__name__)


class GoldLayerManager:
    def __init__(self, spark: SparkSession, cm: ConfigurationManager):
        self.spark = spark
        self.cm = cm

    def _drop_metadata(self, df: DataFrame) -> DataFrame:
        """
        Drop all metadata columns from DataFrame

        :param df: Input DataFrame
        :return: DataFrame without metadata columns
        """

        metadata_cols = [
            "ingestion_timestamp", "ingestion_date",
            "run_id", "source_system", "source_file"
        ]
        existing_metadata = [col for col in metadata_cols if col in df.columns]
        if existing_metadata:
            logger.debug(f"Dropping metadata columns: {existing_metadata}")
        return df.drop(*existing_metadata)

    def _compute_delay_minutes(self, df: DataFrame) -> DataFrame:
        """
        Compute the delay in minutes
        When the delay is negative, the shipment arrived early

        :param df: Input DataFrame
        :return: DataFrame with added columns `delay_minutes` & `arrival_status`
        """
        return df.withColumn(
            "delay_minutes",
            F.round(
                (
                    F.col("norm_actual_arrival").cast("long")
                    - F.col("norm_planned_arrival").cast("long")
                )
                / 60,
                2,
            ),
        ).withColumn(
            "arrival_status",
            F.when(F.col("delay_minutes") > 0, "LATE")
            .when(F.col("delay_minutes") < 0, "EARLY")
            .when(
                F.col("delay_minutes").isNull(), "NULL"
            )
            .otherwise("ON_TIME"),
        )

    def _compute_emission_kg(self, df: DataFrame) -> DataFrame:
        """
        Compute the emission per kg for a shipment

        :param df: Input DataFrame
        :return: DataFrame with column `emission_kg` containing the computed emission per kg
        """

        return df.withColumn("emission_kg", (F.col("distance_km") * F.col("emission_kg_per_km")))

    def _compute_time_efficiency(self, df: DataFrame) -> DataFrame:
        """
        Compute a shipment's time efficiency (trip duration over distance).
        Edge cases: avg_speed_kmh=0 or NULL, distance_km=0 → expected_minutes NULL;
        expected_minutes=0 or NULL → efficiency_score NULL to avoid division by zero.

        :param df: Input DataFrame
        :return: DataFrame with columns `expected_minutes`, `delay_pct`, `efficiency_score`
        """
        return (
            df.withColumn(
                "expected_minutes",
                F.when(
                    (F.col("avg_speed_kmh").isNotNull())
                    & (F.col("avg_speed_kmh") > 0)
                    & (F.col("distance_km").isNotNull())
                    & (F.col("distance_km") >= 0),
                    F.col("distance_km") / F.col("avg_speed_kmh") * 60,
                ),
            )
            .withColumn(
                "delay_pct",
                F.when(
                    (F.col("expected_minutes").isNotNull())
                    & (F.col("expected_minutes") > 0),
                    F.col("delay_minutes") / F.col("expected_minutes"),
                ),
            )
            .withColumn(
                "efficiency_score",
                F.when(
                    F.col("delay_pct").isNotNull(),
                    F.least(
                        F.lit(100),
                        F.greatest(F.lit(0), F.round(100 * (1 - F.col("delay_pct")), 2)),
                    ),
                ),
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

        routes_source_path = self.cm.get_layer_path(source_layer, "routes")
        routes_df = self.spark.read.format(output_format).load(routes_source_path)

        gold_cols = [
            "shipment_id", "route_id", "vehicle_id", "carrier_id",
            "origin_region", "destination_region", "origin_city", "destination_city",
            "vehicle_type", "fuel_type",
            "ship_date", "planned_arrival", "actual_arrival",
            "distance_km", "avg_speed_kmh", "toll_eur",
            "weight_kg", "volume_m3",
            "delay_minutes", "arrival_status", "emission_kg", "efficiency_score",
        ]

        output_path = f"{self.cm.get_bucket('gold')}/{self.cm.get('gold', 'route_performance', default='route_performance')}"

        df = (
            shipments_df
            .join(vehicles_df, on="vehicle_id", how="left")
            .join(routes_df, on="route_id", how="left")
            .transform(lambda df: self._compute_emission_kg(df))
            .transform(lambda df: self._compute_delay_minutes(df))
            .transform(lambda df: self._compute_time_efficiency(df))
            # Write to Golden Delta table
            .select(gold_cols)
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy(["ship_date", "origin_region"])
            .save(output_path)
         )
