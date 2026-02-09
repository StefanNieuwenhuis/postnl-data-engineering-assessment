import pytest
import yaml
from pyspark import Row

from core.configuration_manager import ConfigurationManager
from layers.bronze_layer_manager import BronzeLayerManager


@pytest.fixture
def bronze_config_yaml(tmp_path) -> str:
    """
    A minimal valid YAML config (buckets + datasets)

    :return: (temp) path to configuration YAML
    """

    base = tmp_path / "delta-lake"
    base.mkdir()

    (base / "landing").mkdir()
    (base / "bronze").mkdir()

    config = {
        "storage": {
            "local": {
                "buckets": {"landing": str(base / "landing"), "bronze": str(base / "bronze")}
            },
            "databricks": {"buckets": {}},
        },
        "datasets": {
            "routes": {
                "source": "sources/routes.json",
                "format": "json",
                "stream": "true",
                "bronze_table": "raw_routes",
            },
            "shipments": {
                "source": "sources/shipments.csv",
                "format": "csv",
                "bronze_table": "raw_shipments",
            },
        },
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config), encoding="utf-8")

    return str(config_path)


@pytest.fixture
def sample_csv(tmp_path) -> str:
    """
    Minimal CSV file for batch ingestion tests
    :return: (temp) path to sample CSV file
    """
    csv_path = tmp_path / "shipments.csv"
    csv_path.write_text(
        "shipment_id,route_id,vehicle_id,carrier_id,origin,destination,ship_date,planned_arrival,actual_arrival,weight_kg,volume_m3\n"
        'S1000,R008,V006,CARR01,"Antwerp, BE-Flanders","Groningen, NL-North",2025/09/25 00:00:00,2025-09-25 10:04:01,2025-09-25 11:17:24,645.8,4.33\n'
        'S1001,R002,V001,CARR03,"Lille, FR-North","Duisburg, DE-West",2025-09-25 00:00:00,2025-09-25 05:05:10,2025-09-25 06:16:38,555.1,6.85\n',
        encoding="utf-8",
    )
    return str(csv_path)


@pytest.fixture
def sample_json(tmp_path) -> str:
    """
    Minimal JSON file for streaming ingestion tests

    :return: (temp) path to sample JSON file
    """
    json_path = tmp_path / "routes.json"
    json_path.write_text(
        """[
            {"route_id": "R1", "distance_km": 10},
            {"route_id": "R2", "distance_km": 20},
        ]""",
        encoding="utf-8",
    )

    return str(json_path)


class TestConfigurationManager:
    """Unit tests for ConfigurationManager"""

    class TestMetadata:
        """Unit tests for add metadata, row-preservation, and validation"""

        def test_add_metadata_columns(self, spark_session, bronze_config_yaml) -> None:
            """It should add metadata to a DataFrame successfully"""

            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)
            df = spark_session.createDataFrame([Row(a=1, b="x")])
            run_id = "20260208_120000"
            source = "test_source"

            result = manager._add_metadata(df, run_id, source)

            columns = result.columns

            assert "ingestion_timestamp" in columns
            assert "ingestion_date" in columns
            assert "run_id" in columns
            assert "source_system" in columns
            assert "a" in columns and "b" in columns

        def test_row_preservation(self, spark_session, bronze_config_yaml) -> None:
            """_add_metadata should not change row count"""
            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)
            df = spark_session.createDataFrame([Row(a=1, b="x"), Row(a=2, b="y")])
            original_row_length = df.count()
            run_id = "20260208_120000"
            source = "test_source"

            result = manager._add_metadata(df, run_id, source)

            assert result.count() == original_row_length

        def test_run_id_and_source_system_literal_values(
            self, spark_session, bronze_config_yaml
        ) -> None:
            """run_id and source_system columns should have the given literal values"""

            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)
            df = spark_session.createDataFrame([Row(a=1, b="x")])
            run_id = "20260208_120000"
            source = "test_source"

            result = manager._add_metadata(df, run_id, source)
            row = result.select("run_id", "source_system").first()

            assert row.run_id == run_id
            assert row.source_system == source

    class TestDataIngestion:
        """Unit tests for (streaming) data ingestion"""

        def test_batch_ingestion_csv(self, spark_session, bronze_config_yaml, sample_csv) -> None:
            """Read and ingest a batch data source from CSV and return DataFrame with metadata columns"""

            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)
            source_system = "shipments"
            df = manager._ingest_batch(source_system, sample_csv)

            assert df.count() == 2
            assert "shipment_id" in df.columns and "route_id" in df.columns
            assert "vehicle_id" in df.columns and "carrier_id" in df.columns

        def test_batch_schema_inference(
            self, spark_session, bronze_config_yaml, sample_csv
        ) -> None:
            """Read and ingest a batch data source from CSV"""
            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)
            source_system = "shipments"

            # Ingest without providing explicit schema (should infer)
            df = manager.ingest_batch(source_system, sample_csv, schema=None)

            # Verify schema was inferred correctly
            assert df.count() == 2

            # Check that columns exist (schema was inferred from CSV header)
            expected_columns = [
                "shipment_id",
                "route_id",
                "vehicle_id",
                "carrier_id",
                "origin",
                "destination",
                "ship_date",
                "planned_arrival",
                "actual_arrival",
                "weight_kg",
                "volume_m3",
            ]

            for col in expected_columns:
                assert col in df.columns, f"Expected column '{col}' not found in inferred schema"

            # Verify data types were inferred (not all strings)
            # weight_kg and volume_m3 should be numeric types
            schema_dict = {field.name: field.dataType.simpleString() for field in df.schema.fields}

            # Check that numeric columns were inferred as double/decimal (not string)
            assert schema_dict.get("weight_kg") in [
                "double",
                "decimal(10,1)",
                "decimal(10,2)",
            ], f"weight_kg should be numeric, got {schema_dict.get('weight_kg')}"
            assert schema_dict.get("volume_m3") in [
                "double",
                "decimal(10,1)",
                "decimal(10,2)",
            ], f"volume_m3 should be numeric, got {schema_dict.get('volume_m3')}"

            # Verify metadata columns were added
            assert "ingestion_timestamp" in df.columns
            assert "ingestion_date" in df.columns
            assert "run_id" in df.columns
            assert "source_system" in df.columns
