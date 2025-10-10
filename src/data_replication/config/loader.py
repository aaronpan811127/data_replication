"""
Configuration loader for the data replication system.

This module handles loading and validating YAML configuration files
using Pydantic models.
"""

from pathlib import Path
from typing import Union

import yaml
from pydantic import ValidationError

from data_replication.config.models import ReplicationSystemConfig


class ConfigurationError(Exception):
    """Raised when configuration loading or validation fails."""


class ConfigLoader:
    """Configuration loader for the data replication system."""

    @staticmethod
    def load_from_file(
        config_path: Union[str, Path],
    ) -> ReplicationSystemConfig:
        """
        Load and validate configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            Validated ReplicationSystemConfig instance

        Raises:
            ConfigurationError: If the configuration is invalid
        """
        config_path = Path(config_path)

        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Error reading configuration file: {e}") from e

        if config_data is None:
            raise ConfigurationError(f"Configuration file is empty: {config_path}")

        try:
            config = ReplicationSystemConfig(**config_data)
            return config
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e

    @staticmethod
    def load_from_dict(config_data: dict) -> ReplicationSystemConfig:
        """
        Validate a configuration dictionary.

        Args:
            config_data: Dictionary containing configuration data

        Returns:
            Validated ReplicationSystemConfig instance

        Raises:
            ConfigurationError: If the configuration is invalid
        """
        try:
            config = ReplicationSystemConfig(**config_data)
            return config
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e

    @staticmethod
    def save_to_file(
        config: ReplicationSystemConfig, config_path: Union[str, Path]
    ) -> None:
        """
        Save a configuration to a YAML file.

        Args:
            config: ReplicationSystemConfig instance to save
            config_path: Path where to save the configuration

        Raises:
            ConfigurationError: If saving fails
        """
        config_path = Path(config_path)

        try:
            config_dict = config.model_dump(exclude_none=True)

            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config_dict, f, default_flow_style=False, indent=2)
        except Exception as e:
            raise ConfigurationError(f"Error saving configuration: {e}") from e


# Convenience functions
def load_config(config_path: Union[str, Path]) -> ReplicationSystemConfig:
    """Load configuration from file."""
    return ConfigLoader.load_from_file(config_path)


def validate_config_dict(config_dict: dict) -> ReplicationSystemConfig:
    """Validate configuration dictionary."""
    return ConfigLoader.load_from_dict(config_dict)


def save_config(config: ReplicationSystemConfig, config_path: Union[str, Path]) -> None:
    """Save configuration to file."""
    return ConfigLoader.save_to_file(config, config_path)
