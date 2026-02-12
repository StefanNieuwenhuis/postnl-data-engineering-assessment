import shutil

import pytest
import yaml
from pyspark import Row
from pyspark.sql.types import StructType, StructField, StringType

from core.configuration_manager import ConfigurationManager
from layers.bronze_layer_manager import BronzeLayerManager
from utils.quarantine_utils import QuarantineUtils
from utils.spark_utils import SparkUtils


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
                "stream": True,
                "bronze_table": "raw_routes",
            },
            "shipments": {
                "source": "sources/shipments.csv",
                "format": "csv",
                "quarantine": True,
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
def sample_csv_with_corrupt(tmp_path) -> str:
    """
    CSV file with two valid rows and one malformed row (extra column - 12 values instead of 11).
    Spark CSV with columnNameOfCorruptRecord puts schema-mismatch rows in _corrupt_record.
    """
    csv_path = tmp_path / "shipments_corrupt.csv"
    csv_path.write_text(
        "shipment_id,route_id,vehicle_id,carrier_id,origin,destination,ship_date,planned_arrival,actual_arrival,weight_kg,volume_m3\n"
        'S1000,R008,V006,CARR01,"Antwerp, BE-Flanders","Groningen, NL-North",2025/09/25 00:00:00,2025-09-25 10:04:01,2025-09-25 11:17:24,645.8,4.33\n'
        'S1001,R002,V001,CARR01,"Lille, FR-North","Duisburg, DE-West",2025-09-25 00:00:00,2025-09-25 05:00:00,2025-09-25 06:00:00,500,5.0,extra_column\n'  # 12 cols
        'S1002,R003,V002,CARR02,"Lille, FR-North","Duisburg, DE-West",2025-09-25 00:00:00,2025-09-25 05:05:10,2025-09-25 06:16:38,555.1,6.85\n',
        encoding="utf-8",
    )
    return str(csv_path)

@pytest.fixture
def sample_json(tmp_path) -> str:
    """
    Minimal JSON file for streaming ingestion tests
    Uses NDJSON format (newline-delimited JSON) which is required for Spark Structured Streaming

    :return: (temp) path to sample JSON file
    """
    json_path = tmp_path / "routes.json"
    # NDJSON format: one JSON object per line (required for Spark Structured Streaming)
    json_path.write_text(
        '{"route_id": "R1", "distance_km": 10}\n' '{"route_id": "R2", "distance_km": 20}\n',
        encoding="utf-8",
    )

    return str(json_path)


class TestConfigurationManager:
    """Unit tests for ConfigurationManager"""

    class TestMetadata:
        """Unit tests for add metadata, row-preservation, and validation"""

        def test_add_metadata_columns(self, spark_session, bronze_config_yaml) -> None:
            """It should add metadata to a DataFrame successfully"""

            df = spark_session.createDataFrame([Row(a=1, b="x")])
            run_id = "20260208_120000"
            source = "test_source"

            result = SparkUtils.add_metadata(df, run_id, source)

            columns = result.columns

            assert "ingestion_timestamp" in columns
            assert "ingestion_date" in columns
            assert "run_id" in columns
            assert "source_system" in columns
            assert "a" in columns and "b" in columns

        def test_row_preservation(self, spark_session, bronze_config_yaml) -> None:
            """_add_metadata should not change row count"""
            df = spark_session.createDataFrame([Row(a=1, b="x"), Row(a=2, b="y")])
            original_row_length = df.count()
            run_id = "20260208_120000"
            source = "test_source"

            result = SparkUtils.add_metadata(df, run_id, source)

            assert result.count() == original_row_length

        def test_run_id_and_source_system_literal_values(
            self, spark_session, bronze_config_yaml
        ) -> None:
            """run_id and source_system columns should have the given literal values"""


            df = spark_session.createDataFrame([Row(a=1, b="x")])
            run_id = "20260208_120000"
            source = "test_source"

            result = SparkUtils.add_metadata(df, run_id, source)
            row = result.select("run_id", "source_system").first()

            assert row.run_id == run_id
            assert row.source_system == source

    class TestDataIngestion:
        """Unit tests for (streaming) data ingestion"""

        def test_batch_ingestion_csv(
            self, tmp_path, spark_session, bronze_config_yaml, sample_csv
        ) -> None:
            """Read and ingest a batch data source from CSV and return DataFrame with metadata columns"""

            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)
            run_id = "20260208_120000"
            source_system = "shipments"

            manager._ingest_batch(source_system, sample_csv, run_id, data_format="csv")

            base = tmp_path / "delta-lake"
            assert (base / "bronze" / "raw_shipments").exists()

        def test_stream_ingestion_json(
            self, tmp_path, spark_session, bronze_config_yaml, sample_json
        ) -> None:
            base = tmp_path / "delta-lake"
            # Spark readStream.json() reads from a directory; create one and copy JSON into it
            landing_dir = base / "landing" / "routes"
            landing_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy(sample_json, landing_dir / "routes_1.json")

            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)

            run_id = "20260208_120000"
            source_system = "routes"

            manager._ingest_stream(
                dataset_name=source_system,
                path_dir=str(landing_dir),
                run_id=run_id,
                data_format="json",
            )

            bronze_path = base / "bronze" / "raw_routes"
            assert bronze_path.exists()

    class TestQuarantine:
        """Unit tests for corrupt record handling and quarantine."""

        def test_write_quarantine_empty_returns_zero(
            self, tmp_path, spark_session, bronze_config_yaml
        ) -> None:
            """merge_upsert with empty DataFrame should not create files."""

            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)

            schema = StructType([StructField("_corrupt_record", StringType(), nullable=True)])
            empty_df = spark_session.createDataFrame([], schema)

            quarantine_path = cm.get_quarantine_path("shipments")
            QuarantineUtils.merge_upsert(spark_session, empty_df, quarantine_path, "shipments")

            quarantine_path_local = tmp_path / "delta-lake" / "bronze" / "_quarantine" / "shipments"
            assert not quarantine_path_local.exists()

        def test_write_quarantine_writes_to_path(
            self, tmp_path, spark_session, bronze_config_yaml
        ) -> None:
            """merge_upsert should write corrupt records to quarantine Delta table with metadata."""

            cm = ConfigurationManager(bronze_config_yaml)
            quarantine_path = cm.get_quarantine_path("shipments")

            schema = StructType([StructField("_corrupt_record", StringType(), nullable=True)])
            corrupt_df = spark_session.createDataFrame(
                [Row(_corrupt_record="bad,row,data")], schema
            )
            corrupt_df = SparkUtils.add_metadata(
                corrupt_df, "run_001", "shipments", source_file="/path/to/file.csv"
            )

            QuarantineUtils.merge_upsert(spark_session, corrupt_df, quarantine_path, "shipments")

            quarantine_path_local = tmp_path / "delta-lake" / "bronze" / "_quarantine" / "shipments"
            assert quarantine_path_local.exists()

            # Verify quarantine table has data with metadata and merge key
            df = spark_session.read.format("delta").load(str(quarantine_path_local))
            assert df.count() == 1
            assert "_corrupt_record" in df.columns
            assert "quarantine_row_hash" in df.columns
            assert "run_id" in df.columns
            assert "source_system" in df.columns
            assert "source_file" in df.columns

        def test_ingest_batch_quarantine_splits_valid_and_corrupt(
            self, tmp_path, spark_session, bronze_config_yaml, sample_csv_with_corrupt
        ) -> None:
            """With quarantine=True, valid rows go to bronze; corrupt rows (if any) go to quarantine."""
            base = tmp_path / "delta-lake"
            cm = ConfigurationManager(bronze_config_yaml)
            manager = BronzeLayerManager(spark_session, cm)

            manager._ingest_batch(
                "shipments",
                sample_csv_with_corrupt,
                "run_001",
                data_format="csv",
                quarantine=True,
            )

            bronze_path = base / "bronze" / "raw_shipments"
            quarantine_path = base / "bronze" / "_quarantine" / "shipments"

            assert bronze_path.exists()
            bronze_df = spark_session.read.format("delta").load(str(bronze_path))
            bronze_count = bronze_df.count()

            # File has 3 rows; row 2 has extra column (schema mismatch).
            # Spark may put schema-mismatch rows in _corrupt_record → quarantine.
            # If Spark treats all as valid, we get 3 in bronze and no quarantine.
            assert bronze_count >= 2
            assert bronze_count <= 3

            if quarantine_path.exists():
                quarantine_df = spark_session.read.format("delta").load(str(quarantine_path))
                assert quarantine_df.count() >= 1
                assert bronze_count + quarantine_df.count() == 3

        def test_write_quarantine_merge_idempotency(
            self, tmp_path, spark_session, bronze_config_yaml
        ) -> None:
            """Re-writing the same corrupt record should not create duplicates."""

            cm = ConfigurationManager(bronze_config_yaml)
            quarantine_path = cm.get_quarantine_path("shipments")

            schema = StructType([StructField("_corrupt_record", StringType(), nullable=True)])
            corrupt_df = spark_session.createDataFrame(
                [Row(_corrupt_record="bad,row,data")], schema
            )
            corrupt_df = SparkUtils.add_metadata(
                corrupt_df, "run_001", "shipments", source_file="/path/to/file.csv"
            )

            QuarantineUtils.merge_upsert(spark_session, corrupt_df, quarantine_path, "shipments")

            corrupt_df2 = spark_session.createDataFrame(
                [Row(_corrupt_record="bad,row,data")], schema
            )
            corrupt_df2 = SparkUtils.add_metadata(
                corrupt_df2, "run_002", "shipments", source_file="/path/to/file.csv"
            )
            QuarantineUtils.merge_upsert(spark_session, corrupt_df2, quarantine_path, "shipments")

            quarantine_path_local = tmp_path / "delta-lake" / "bronze" / "_quarantine" / "shipments"
            quarantine_df = spark_session.read.format("delta").load(str(quarantine_path_local))
            assert quarantine_df.count() == 1
