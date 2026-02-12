import pytest
import yaml
from pyspark import Row
from pyspark.sql import functions as F

from core.configuration_manager import ConfigurationManager
from layers.silver_layer_manager import SilverLayerManager, QUARANTINE_REASON_COLUMN


@pytest.fixture
def silver_config_yaml(tmp_path) -> str:
    """
    Minimal YAML config with bronze + silver buckets, datasets, and silver transformations.

    :return: Path to the config YAML file.
    """
    base = tmp_path / "delta-lake"
    base.mkdir()
    (base / "bronze").mkdir()
    (base / "silver").mkdir()

    config = {
        "storage": {
            "local": {
                "buckets": {
                    "landing": str(base / "landing"),
                    "bronze": str(base / "bronze"),
                    "silver": str(base / "silver"),
                }
            },
            "databricks": {"buckets": {}},
        },
        "datasets": {
            "routes": {
                "source": "sources/routes.json",
                "format": "json",
                "bronze_table": "raw_routes",
                "silver_table": "clean_routes",
            },
            "shipments": {
                "source": "sources/shipments.csv",
                "format": "csv",
                "bronze_table": "raw_shipments",
                "silver_table": "clean_shipments",
            },
        },
        "transformations": {
            "silver": {
                "routes": {
                    "dedupe_keys": ["route_id", "ingestion_timestamp"],
                    "required_columns": ["route_id"],
                    "date_columns": [],
                    "datetime_columns": [],
                },
                "shipments": {
                    "dedupe_keys": ["shipment_id"],
                    "required_columns": ["shipment_id", "route_id"],
                    "date_columns": ["ship_date"],
                    "datetime_columns": ["planned_arrival", "actual_arrival"],
                },
            },
        },
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config), encoding="utf-8")
    return str(config_path)


class TestSilverLayerManager:
    """Unit tests for SilverLayerManager."""

    class TestNormalizeTimestamps:
        """Tests for _normalize_timestamps."""

        def test_adds_normalized_date_columns(self, spark_session, silver_config_yaml) -> None:
            """Adds norm_* columns for configured date_columns."""
            cm = ConfigurationManager(silver_config_yaml)
            manager = SilverLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(
                        ship_date="2025-09-25",
                        planned_arrival="2025-09-25 10:00:00",
                        actual_arrival="2025-09-25 11:00:00",
                    )
                ]
            )

            result = manager._normalize_timestamps(df, "shipments")

            assert "norm_ship_date" in result.columns
            assert "norm_planned_arrival" in result.columns

        def test_empty_config_returns_unchanged(self, spark_session, silver_config_yaml) -> None:
            """When no date/datetime columns in config, no new columns added."""
            cm = ConfigurationManager(silver_config_yaml)
            manager = SilverLayerManager(spark_session, cm)

            df = spark_session.createDataFrame([Row(a=1, b="x")])

            result = manager._normalize_timestamps(df, "routes")

            assert result.columns == ["a", "b"]

    class TestDeduplicate:
        """Tests for _deduplicate."""

        def test_empty_key_cols_returns_unchanged(self, spark_session, silver_config_yaml) -> None:
            """When key_cols is None or empty, DataFrame is unchanged."""
            cm = ConfigurationManager(silver_config_yaml)
            manager = SilverLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(ingestion_timestamp="2025-09-25 10:00:00", id=1),
                    Row(ingestion_timestamp="2025-09-25 10:01:00", id=2),
                ]
            )

            kept_none, dropped_none = manager._deduplicate(df, key_cols=None)
            kept_empty, dropped_empty = manager._deduplicate(df, key_cols=[])

            assert kept_none.count() == 2
            assert kept_empty.count() == 2

    class TestHandleMissingValues:
        """Tests for _handle_missing_values."""

        def test_empty_key_cols_returns_unchanged(self, spark_session, silver_config_yaml) -> None:
            """When key_cols is None or empty, no rows dropped."""
            cm = ConfigurationManager(silver_config_yaml)
            manager = SilverLayerManager(spark_session, cm)

            df = spark_session.createDataFrame([Row(a=1, b=None), Row(a=2, b="y")])

            valid_df, invalid_df = manager._handle_missing_values(df, key_cols=None)

            assert valid_df.count() == 2

        def test_drops_rows_with_nulls_in_key_columns(
            self, spark_session, silver_config_yaml
        ) -> None:
            """Rows with null in any of key_cols are dropped."""
            cm = ConfigurationManager(silver_config_yaml)
            manager = SilverLayerManager(spark_session, cm)

            df = spark_session.createDataFrame(
                [
                    Row(shipment_id="S1", route_id="R1"),
                    Row(shipment_id="S2", route_id=None),
                    Row(shipment_id="S3", route_id="R3"),
                ]
            )

            valid_df, invalid_df = manager._handle_missing_values(df, key_cols=["shipment_id", "route_id"])

            assert valid_df.count() == 2
            rows = valid_df.collect()
            ids = [r.shipment_id for r in rows]
            assert "S2" not in ids
