import logging
import os
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)

# Mapping of {[layer name] -> dataset configuration key} for path within a layer's bucket
LAYER_DATASET_KEYS: Dict[str, str] = {
    "landing": "source",
    "bronze": "bronze_table",
    "silver": "silver_table",
    "gold": "gold_table",
}


class ConfigurationManager:
    """This class is the main interface between configuration YAMLs and the application"""

    def __init__(self, config_path: str = "config/config.yaml") -> None:
        """
        Initialize ConfigurationManager instance with configuration

        :param config_path: Path to the configuration YAML
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.environment = self._detect_environment()

        logger.info(f"Configuration loaded successfully")

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML

        :raise: FileNotFoundError when no configuration file is found at the path provided
        :raise: YamlError when the YAML file is corrupt - i.e. formatted incorrectly
        :return: Dictionary of configurations
        """
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML: {e}")
            raise

    def _detect_environment(self) -> str:
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            return "databricks"
        else:
            return self.config.get("environment", {}).get("name", "local")

    def get_environment(self) -> str:
        """
        Getter that retrieves the currently active environment
        """
        return self.environment

    def get(self, *keys, default=None) -> Any:
        """
        Get a nested configuration value by traversing keys.

        :param keys: Nested keys to traverse (e.g. `storage` => `local` => `buckets` => `landing` => `value`).
        :param default: Value returned if any key is missing.
        :return: Configuration value at the given path, or default.
        """

        value = self.config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, default)
            else:
                return default
        return value

    def get_bucket(self, data_layer: str) -> str:
        """
        Retrieve the bucket location from configuration

        :param data_layer: Data layer name
        :return: Bucket location
        :raises KeyError: When the layer and/or bucket is unavailable in the configuration
        """

        bucket = self.get("storage", self.environment, "buckets", data_layer)

        if not bucket:
            raise KeyError(f"Unknown layer or missing bucket: {data_layer}")

        return str(bucket)

    def get_layer_path(self, data_layer: str, dataset_name: str) -> str:
        """
        Retrieve full path for a dataset in a given data layer (bucket + path within bucket).

        :param data_layer: Desired data layer (`landing`, `bronze`, `silver`, `gold`)
        :param dataset_name: Key in datasets
        :return: Full path string for a dataset in the data layer
        """
        key = LAYER_DATASET_KEYS.get(data_layer)

        if not key:
            raise KeyError(
                f"Unknown data layer: {data_layer}. Use one of: {list(LAYER_DATASET_KEYS)}"
            )

        # Reject missing or blank path; empty or whitespace-only string yields bucket/ or bucket// - i.e. invalid dataset path
        rel = self.get("datasets", dataset_name, key)
        if rel is None or (isinstance(rel, str) and not rel.strip()):
            raise KeyError(f"Dataset {dataset_name} has no path for layer {data_layer} (key {key}")

        bucket = self.get_bucket(data_layer)

        return f"{bucket}/{str(rel)}"

    def get_datasets(self) -> Dict[str, Any]:
        """
        Get the datasets configuration (source, format, {layer}_table, etc. per dataset)

        :return: Dict of {[dataset name] -> {source, format, ...}}
        """

        return self.get("datasets")

    def get_transformations(self, layer: str, dataset_name: str) -> Dict[str, Any]:
        """
        Get transformation configuration for a dataset in a layer

        :param layer: Data Layer name
        :param dataset_name: Dataset name
        :return: Dict of transformations
        """

        return self.get("transformations", layer, dataset_name)
