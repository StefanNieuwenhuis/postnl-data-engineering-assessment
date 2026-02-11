from typing import Optional

import pytest
import yaml
from pyspark import Row
from pyspark.sql import functions as F

from core.configuration_manager import ConfigurationManager
from layers.gold_layer_manager import GoldLayerManager


@pytest.fixture
def gold_config_yaml(tmp_path) -> str:
    """
    Minimal YAML config with silver + gold buckets and datasets for shipments/vehicles/routes.

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
            "routes": {
                "source": "sources/routes.json",
                "format": "json",
                "bronze_table": "raw_routes",
                "silver_table": "clean_routes",
                "gold_table": "kpi_routes",
            },
        },
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config), encoding="utf-8")
    return str(config_path)


def _make_delay_df(spark_session, planned_ts: str, actual_ts: Optional[str]):
    """Create DataFrame with norm_planned_arrival and norm_actual_arrival (gold layer expects these)."""
    df = spark_session.createDataFrame(
        [
            Row(
                shipment_id="S1",
                norm_planned_arrival=planned_ts,
                norm_actual_arrival=actual_ts,
            ),
        ]
    )
    df = df.withColumn(
        "norm_planned_arrival", F.to_timestamp(F.col("norm_planned_arrival"))
    )
    if actual_ts is not None:
        df = df.withColumn(
            "norm_actual_arrival", F.to_timestamp(F.col("norm_actual_arrival"))
        )
    else:
        df = df.withColumn(
            "norm_actual_arrival", F.lit(None).cast("timestamp")
        )
    return df


class TestGoldLayerManager:
    """Unit tests for GoldLayerManager."""

    class TestDropMetadata:
        """Tests for _drop_metadata."""

        def test_drops_metadata_columns(self, spark_session, gold_config_yaml) -> None:
            """Removes ingestion_timestamp, ingestion_date, run_id, source_system, source_file."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(
                        a=1,
                        ingestion_timestamp="2025-09-25 10:00:00",
                        run_id="run1",
                        source_system="shipments",
                    ),
                ]
            )

            result = manager._drop_metadata(df)

            assert "ingestion_timestamp" not in result.columns
            assert "run_id" not in result.columns
            assert "source_system" not in result.columns
            assert "a" in result.columns

        def test_unchanged_when_no_metadata(self, spark_session, gold_config_yaml) -> None:
            """Returns DataFrame unchanged when no metadata columns exist."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            df = spark_session.createDataFrame([Row(a=1, b="x")])

            result = manager._drop_metadata(df)

            assert result.columns == ["a", "b"]

    class TestComputeDelayMinutes:
        """Tests for _compute_delay_minutes (expects norm_planned_arrival, norm_actual_arrival)."""

        def test_adds_delay_minutes_and_arrival_status_columns(
            self, spark_session, gold_config_yaml
        ) -> None:
            """Adds delay_minutes and arrival_status columns."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)
            df = _make_delay_df(spark_session, "2025-09-25 10:00:00", "2025-09-25 11:00:00")

            result = manager._compute_delay_minutes(df)

            assert "delay_minutes" in result.columns
            assert "arrival_status" in result.columns

        def test_late_arrival_status(self, spark_session, gold_config_yaml) -> None:
            """arrival_status is LATE when norm_actual_arrival > norm_planned_arrival."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)
            df = _make_delay_df(spark_session, "2025-09-25 10:00:00", "2025-09-25 11:00:00")

            result = manager._compute_delay_minutes(df)
            row = result.select("arrival_status").first()

            assert row.arrival_status == "LATE"

        def test_early_arrival_status(self, spark_session, gold_config_yaml) -> None:
            """arrival_status is EARLY when norm_actual_arrival < norm_planned_arrival."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)
            df = _make_delay_df(spark_session, "2025-09-25 11:00:00", "2025-09-25 10:00:00")

            result = manager._compute_delay_minutes(df)
            row = result.select("arrival_status").first()

            assert row.arrival_status == "EARLY"

        def test_on_time_arrival_status(self, spark_session, gold_config_yaml) -> None:
            """arrival_status is ON_TIME when norm_actual_arrival == norm_planned_arrival."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)
            df = _make_delay_df(spark_session, "2025-09-25 10:00:00", "2025-09-25 10:00:00")

            result = manager._compute_delay_minutes(df)
            row = result.select("arrival_status", "delay_minutes").first()

            assert row.arrival_status == "ON_TIME"
            assert row.delay_minutes == 0


    class TestComputeEmissionKg:
        """Tests for _compute_emission_kg."""

        def test_adds_emission_kg_column(self, spark_session, gold_config_yaml) -> None:
            """Computes emission_kg = distance_km * emission_kg_per_km."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(
                        shipment_id="S1",
                        distance_km=100.0,
                        emission_kg_per_km=0.5,
                    ),
                ]
            )

            result = manager._compute_emission_kg(df)
            row = result.select("emission_kg").first()

            assert row.emission_kg == 50.0

    class TestComputeTimeEfficiency:
        """Tests for _compute_time_efficiency."""

        def test_adds_efficiency_score_and_related_columns(
            self, spark_session, gold_config_yaml
        ) -> None:
            """Adds expected_minutes, delay_pct, efficiency_score."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(
                        shipment_id="S1",
                        distance_km=100.0,
                        avg_speed_kmh=50.0,
                        delay_minutes=10.0,
                    ),
                ]
            )

            result = manager._compute_time_efficiency(df)

            assert "expected_minutes" in result.columns
            assert "delay_pct" in result.columns
            assert "efficiency_score" in result.columns

        def test_efficiency_score_bounded_between_0_and_100(
            self, spark_session, gold_config_yaml
        ) -> None:
            """efficiency_score is clamped between 0 and 100."""
            cm = ConfigurationManager(gold_config_yaml)
            manager = GoldLayerManager(spark_session, cm)

            # On time: expected_minutes=60, delay_minutes=0 -> delay_pct=0, score=100
            df = spark_session.createDataFrame(
                [
                    Row(
                        shipment_id="S1",
                        distance_km=100.0,
                        avg_speed_kmh=100.0,
                        delay_minutes=0.0,
                    ),
                ]
            )

            result = manager._compute_time_efficiency(df)
            row = result.select("efficiency_score").first()

            assert row.efficiency_score == 100.0

