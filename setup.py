"""
setup.py configuration script for data_replication project.

A comprehensive data replication system for Databricks with support for
backup, delta sharing, replication, and reconciliation of DLT tables.
"""

import datetime
from pathlib import Path

from setuptools import find_packages, setup

local_version = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d.%H%M%S")

setup(
    name="data_replication",
    version="1.0.0" + "+" + local_version,
    url="https://databricks.com",
    description="Data replication system for Databricks tables",
    long_description=Path("./src/data_replication/README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    packages=find_packages(where="./src"),
    package_dir={"": "src/"},
    entry_points={
        "console_scripts": [
            "data-replicator=data_replication.cli.main:main",
        ],
    },
    install_requires=[
        "databricks-connect==17.1.*",
        "pydantic>=2.0.0",
        "pyyaml>=6.0",
        "click>=8.0.0",
        "databricks-sdk>=0.8.0",
        "requests>=2.28.0",
        "tenacity>=8.0.0",
        "structlog>=22.0.0",
        "typer>=0.9.0",
        "rich>=13.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-mock>=3.10.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "coverage>=7.0.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-mock>=3.10.0",
            "pytest-asyncio>=0.21.0",
            "coverage>=7.0.0",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
