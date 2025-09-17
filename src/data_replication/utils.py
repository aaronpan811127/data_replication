"""
Retry utilities for data replication operations.

This module provides retry functionality with exponential backoff
and configurable retry strategies.
"""

import os
import time
from functools import wraps
from typing import Optional

from databricks.connect import DatabricksSession

from data_replication.audit.logger import DataReplicationLogger
from data_replication.config.models import RetryConfig


def create_spark_session(config, host, token) -> DatabricksSession:
    """Create Spark session configured for workspace."""

    # Note: In production, tokens should be retrieved from Databricks secrets
    os.environ["DATABRICKS_HOST"] = config.host
    os.environ["DATABRICKS_HOST"] = host
    os.environ["DATABRICKS_TOKEN"] = token
    # Create Databricks session with serverless compute
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

    return spark


def retry_with_logging(
    retry_config: RetryConfig, logger: Optional[DataReplicationLogger] = None
):
    """
    Decorator for retrying operations with logging.

    Args:
        retry_config: RetryConfig object with retry settings
        logger: Optional logger for logging attempts

    Returns:
        Decorated function with retry logic and logging
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = retry_config.retry_delay_seconds

            for attempt in range(1, retry_config.max_attempts + 1):
                try:
                    if logger and hasattr(logger, "debug"):
                        logger.debug(
                            f"Attempting {func.__name__} (attempt {attempt}/{retry_config.max_attempts})"
                        )

                    result = func(*args, **kwargs)

                    logger.info(
                        f"{func.__name__} succeeded on attempt {attempt}/{retry_config.max_attempts}"
                    )

                    return result, None, attempt, retry_config.max_attempts

                except Exception as e:
                    last_exception = e

                    logger.warning(
                        f"{func.__name__} failed on attempt {attempt}/{retry_config.max_attempts}: {str(e)}"
                    )

                    if attempt < retry_config.max_attempts:
                        logger.debug(f"Waiting {current_delay:.1f}s before retry...")
                        time.sleep(current_delay)
                        current_delay *= 2.0  # Exponential backoff
                    else:
                        logger.error(
                            f"{func.__name__} failed after {retry_config.max_attempts} attempts"
                        )

            return False, last_exception, attempt, retry_config.max_attempts

        return wrapper

    return decorator
