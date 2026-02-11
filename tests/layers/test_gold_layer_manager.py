import pytest
import yaml
from pyspark import Row
from pyspark.sql import functions as F

from core.configuration_manager import ConfigurationManager
from layers.gold_layer_manager import GoldLayerManager


@pytest.fixture
def gold_config_yaml(tmp_path) -> str:
    """
    Minimal YAML config with silver + gold buckets and datasets for shipments/vehicles.

    :return: Path to the config YAML file.
    """
    base = tmp_path / "delta-lake"
    base.mkdir()
    (base / "silver").mkdir()
    (base / "gold").mkdir()

    config = {
        "storage": {
            "local": {
                "buckets": {
                    "silver": str(base / "silver"),
                    "gold": str(base / "gold"),
                }
            },
            "databricks": {"buckets": {}},
        },
        "datasets": {
            "shipments": {
                "source": "sources/shipments.csv",
                "format": "csv",
                "bronze_table": "raw_shipments",
                "silver_table": "clean_shipments",
                "gold_table": "kpi_shipments",
            },
            "vehicles": {
                "source": "sources/vehicles.csv",
                "format": "csv",
                "bronze_table": "raw_vehicles",
                "silver_table": "clean_vehicles",
                "gold_table": "kpi_vehicles",
            },
        },
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config), encoding="utf-8")
    return str(config_path)


class TestGoldLayerManager:
    """Unit tests for GoldLayerManager."""

    class TestComputeDelayMinutes:
        """Tests for _compute_delay_minutes."""

        def test_adds_delay_minutes_and_arrival_status_columns(
            self, spark_session, gold_config_yaml
        ) -> None:
            """Adds delay_minutes and arrival_status columns."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(
                        shipment_id="S1",
                        planned_arrival="2025-09-25 10:00:00",
                        actual_arrival="2025-09-25 11:00:00",
                    ),
                ]
            )
            df = df.withColumn(
                "planned_arrival", F.to_timestamp(F.col("planned_arrival"))
            ).withColumn("actual_arrival", F.to_timestamp(F.col("actual_arrival")))

            result = manager._compute_delay_minutes(df)

            assert "delay_minutes" in result.columns
            assert "arrival_status" in result.columns

        def test_late_arrival_status(
            self, spark_session, gold_config_yaml
        ) -> None:
            """arrival_status is LATE when actual_arrival > planned_arrival."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(
                        shipment_id="S1",
                        planned_arrival="2025-09-25 10:00:00",
                        actual_arrival="2025-09-25 11:00:00",
                    ),
                ]
            )
            df = df.withColumn(
                "planned_arrival", F.to_timestamp(F.col("planned_arrival"))
            ).withColumn("actual_arrival", F.to_timestamp(F.col("actual_arrival")))

            result = manager._compute_delay_minutes(df)
            row = result.select("arrival_status").first()

            assert row.arrival_status == "LATE"

        def test_early_arrival_status(
            self, spark_session, gold_config_yaml
        ) -> None:
            """arrival_status is EARLY when actual_arrival < planned_arrival."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(
                        shipment_id="S1",
                        planned_arrival="2025-09-25 11:00:00",
                        actual_arrival="2025-09-25 10:00:00",
                    ),
                ]
            )
            df = df.withColumn(
                "planned_arrival", F.to_timestamp(F.col("planned_arrival"))
            ).withColumn("actual_arrival", F.to_timestamp(F.col("actual_arrival")))

            result = manager._compute_delay_minutes(df)
            row = result.select("arrival_status").first()

            assert row.arrival_status == "EARLY"

        def test_on_time_arrival_status(
            self, spark_session, gold_config_yaml
        ) -> None:
            """arrival_status is ON_TIME when actual_arrival == planned_arrival."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(
                        shipment_id="S1",
                        planned_arrival="2025-09-25 10:00:00",
                        actual_arrival="2025-09-25 10:00:00",
                    ),
                ]
            )
            df = df.withColumn(
                "planned_arrival", F.to_timestamp(F.col("planned_arrival"))
            ).withColumn("actual_arrival", F.to_timestamp(F.col("actual_arrival")))

            result = manager._compute_delay_minutes(df)
            row = result.select("arrival_status", "delay_minutes").first()

            assert row.arrival_status == "ON_TIME"
            assert row.delay_minutes == 0
