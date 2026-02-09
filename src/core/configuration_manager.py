import logging
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