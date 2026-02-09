import logging
import os
from typing import Dict, Any

import yaml

logger = logging.getLogger(__name__)

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