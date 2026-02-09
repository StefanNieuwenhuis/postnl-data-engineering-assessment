import pytest
import yaml

from core.configuration_manager import ConfigurationManager


@pytest.fixture
def valid_config_yaml(tmp_path) -> str:
    """
    A minimal valid YAML config (buckets + datasets)

    :return: (temp) config_path to configuration YAML
    """
    config = {
        "storage": {
            "local": {
                "buckets": {
                    "landing": "s3a://landing",
                },
            },
            "databricks": {
                "buckets": {
                    "landing": "/mnt/landing",
                },
            },
        },
        "datasets": {
            "routes": {
                "source": "sources/routes",
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
def empty_config_yaml(tmp_path):
    """
    An empty valid YAML config that parses to None - i.e. empty configuration Dict

    :return: (temp) path to configuration YAML
    """
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")
    return str(path)


class TestConfigurationManager:
    """Unit tests for ConfigurationManager"""

    class TestInitialization:
        """Test initialization and configuration loading"""

        def test_init_loads_valid_config(self, valid_config_yaml) -> None:
            """ConfigurationManager initializes with a valid config file, and sets the environment"""
            cm = ConfigurationManager(valid_config_yaml)

            assert cm.config_path == valid_config_yaml
            assert cm.config is not None
            assert "storage" in cm.config
            assert cm.config["storage"]["local"]["buckets"]["landing"] == "s3a://landing"

        def test_init_raises_file_not_found_for_missing_path(self) -> None:
            """ConfigurationManager initializes with a non-existent path"""
            with pytest.raises(FileNotFoundError) as exc_info:
                ConfigurationManager("/nonexistent/config.yaml")
            assert "Configuration file not found" in str(exc_info.value) or "config.yaml" in str(
                exc_info.value
            )

        def test_init_raises_yaml_error_for_invalid_yaml(self, tmp_path) -> None:
            """ConfigurationManager initializes with invalid YAML content"""
            bad_yaml = tmp_path / "bad.yaml"
            bad_yaml.write_text("key: [unclosed\n  - list", encoding="utf-8")
            with pytest.raises(yaml.YAMLError):
                ConfigurationManager(str(bad_yaml))

        def test_init_with_empty_file_fails_environment_detection(self, empty_config_yaml) -> None:
            """Empty YAML file loads as None (safe_load); _detect_environment then fails on None.get()."""
            with pytest.raises(AttributeError):
                ConfigurationManager(empty_config_yaml)

    class TestGetEnvironment:
        """Tests for environment detection"""

        def test_get_environment_returns_detected_environment(self, valid_config_yaml) -> None:
            """get_environment returns the value set at init."""
            cm = ConfigurationManager(valid_config_yaml)
            assert cm._detect_environment() == "local"

    class TestGetBucketsAndLayers:
        """Tests for bucket retrieval"""

        def test_get_bucket(self, valid_config_yaml) -> None:
            """get_bucket returns the bucket for a layer"""
            cm = ConfigurationManager(valid_config_yaml)

            assert cm.get_bucket("landing") == "s3a://landing"

        def test_get_unknown_bucket_raises(self, valid_config_yaml) -> None:
            """get bucket raises KeyError"""
            cm = ConfigurationManager(valid_config_yaml)

            with pytest.raises(KeyError) as exc_info:
                cm.get_bucket("nonexistent_bucket")
            assert "nonexistent_bucket" in str(exc_info.value)

        def test_get_layer_path(self, valid_config_yaml) -> None:
            """get_layer_path returns the bucket + dataset source"""
            cm = ConfigurationManager(valid_config_yaml)
            path = cm.get_layer_path("landing", "routes")

            assert path == "s3a://landing/sources/routes"

        def test_get_layer_path_unknown_dataset_raises(self, valid_config_yaml):
            """get_layer_path with unknown dataset raises KeyError."""
            cm = ConfigurationManager(valid_config_yaml)
            with pytest.raises(KeyError) as exc_info:
                cm.get_layer_path("landing", "nonexistent_table")
            assert "nonexistent_table" in str(exc_info.value)

        def test_get_layer_path_unknown_layer_raises(self, valid_config_yaml):
            """get_layer_path with unknown layer raises KeyError."""
            cm = ConfigurationManager(valid_config_yaml)
            with pytest.raises(KeyError) as exc_info:
                cm.get_layer_path("invalid_layer", "routes")
            assert "invalid_layer" in str(exc_info.value)
