"""Configuration module for Cloud Data Lakehouse Pipeline.

Author: Prabhat Sharma (prabhat84sharma-ui)
Description:
    Centralized configuration management for lakehouse storage paths,
    Apache Spark session tuning, Delta Lake table parameters, AWS S3/MinIO
    credentials, and data quality validation thresholds.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict


@dataclass(frozen=True)
class StorageConfig:
    """Storage paths for Medallion architecture (Bronze, Silver, Gold)."""

    base_path: str = field(
        default_factory=lambda: os.getenv("DATA_LAKE_BASE_PATH", "./lakehouse_storage")
    )
    raw_input_path: str = field(
        default_factory=lambda: os.getenv(
            "RAW_INPUT_PATH", "./data/sample_transactions.csv"
        )
    )
    bronze_path: str = field(
        default_factory=lambda: os.getenv(
            "BRONZE_TABLE_PATH", "./lakehouse_storage/bronze/transactions"
        )
    )
    silver_path: str = field(
        default_factory=lambda: os.getenv(
            "SILVER_TABLE_PATH", "./lakehouse_storage/silver/transactions"
        )
    )
    gold_customer_mart_path: str = field(
        default_factory=lambda: os.getenv(
            "GOLD_CUSTOMER_MART_PATH", "./lakehouse_storage/gold/customer_360"
        )
    )
    gold_regional_mart_path: str = field(
        default_factory=lambda: os.getenv(
            "GOLD_REGIONAL_MART_PATH", "./lakehouse_storage/gold/regional_sales"
        )
    )
    quarantine_path: str = field(
        default_factory=lambda: os.getenv(
            "QUARANTINE_PATH", "./lakehouse_storage/quarantine/transactions"
        )
    )


@dataclass(frozen=True)
class S3Config:
    """AWS S3 or MinIO S3-compatible object store connection parameters."""

    endpoint_url: str = field(
        default_factory=lambda: os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    )
    access_key: str = field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    )
    secret_key: str = field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    )
    region: str = field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1")
    )
    bucket_name: str = field(
        default_factory=lambda: os.getenv("S3_BUCKET_NAME", "lakehouse-data")
    )
    path_style_access: bool = field(
        default_factory=lambda: os.getenv("S3_PATH_STYLE_ACCESS", "true").lower() == "true"
    )
    ssl_enabled: bool = field(
        default_factory=lambda: os.getenv("S3_SSL_ENABLED", "false").lower() == "true"
    )


@dataclass(frozen=True)
class DeltaConfig:
    """Delta Lake table optimization and retention configurations."""

    auto_optimize_write: bool = field(
        default_factory=lambda: os.getenv("DELTA_AUTO_OPTIMIZE_OPTIMIZE_WRITE", "true").lower() == "true"
    )
    auto_compact: bool = field(
        default_factory=lambda: os.getenv("DELTA_AUTO_OPTIMIZE_AUTO_COMPACT", "true").lower() == "true"
    )
    vacuum_retention_hours: int = field(
        default_factory=lambda: int(os.getenv("DELTA_VACUUM_RETENTION_HOURS", "168"))
    )
    partition_columns: list[str] = field(
        default_factory=lambda: ["region", "transaction_date"]
    )


@dataclass(frozen=True)
class SparkConfig:
    """Apache Spark session performance and metastore configurations."""

    app_name: str = field(
        default_factory=lambda: os.getenv("SPARK_APP_NAME", "CloudDataLakehousePipeline")
    )
    master: str = field(
        default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]")
    )
    driver_memory: str = field(
        default_factory=lambda: os.getenv("SPARK_DRIVER_MEMORY", "4g")
    )
    executor_memory: str = field(
        default_factory=lambda: os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    )
    sql_shuffle_partitions: int = field(
        default_factory=lambda: int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
    )
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )


@dataclass(frozen=True)
class BusinessRulesConfig:
    """Domain business rules, currency fx rates, and validation constraints."""

    # Static FX rates relative to USD base currency
    exchange_rates_to_usd: Dict[str, float] = field(
        default_factory=lambda: {
            "USD": 1.0000,
            "EUR": 1.0920,
            "GBP": 1.2840,
            "CAD": 0.7410,
            "INR": 0.0120,
        }
    )
    high_value_threshold_usd: float = 1000.0
    valid_statuses: tuple[str, ...] = ("COMPLETED", "PENDING", "FAILED", "REFUNDED")
    valid_regions: tuple[str, ...] = (
        "US-EAST",
        "US-WEST",
        "EU-WEST",
        "AP-SOUTH",
        "CA-CENTRAL",
    )


@dataclass(frozen=True)
class LakehouseConfig:
    """Master configuration aggregating all pipeline sub-configurations."""

    storage: StorageConfig = field(default_factory=StorageConfig)
    s3: S3Config = field(default_factory=S3Config)
    delta: DeltaConfig = field(default_factory=DeltaConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    business: BusinessRulesConfig = field(default_factory=BusinessRulesConfig)
    environment: str = field(
        default_factory=lambda: os.getenv("ENVIRONMENT", "local").lower()
    )

    def get_spark_parameters(self) -> Dict[str, str]:
        """Generate Spark configuration key-value mappings."""
        configs = {
            "spark.app.name": self.spark.app_name,
            "spark.master": self.spark.master,
            "spark.driver.memory": self.spark.driver_memory,
            "spark.executor.memory": self.spark.executor_memory,
            "spark.sql.shuffle.partitions": str(self.spark.sql_shuffle_partitions),
            # Delta Lake Catalog & Extensions
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # Delta Lake Optimizations
            "spark.databricks.delta.optimizeWrite.enabled": str(
                self.delta.auto_optimize_write
            ).lower(),
            "spark.databricks.delta.autoCompact.enabled": str(
                self.delta.auto_compact
            ).lower(),
            # Fast Parquet Reader and UTC standard time
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.parquet.compression.codec": "snappy",
        }

        # If running in MinIO/S3 cloud mode, inject Hadoop S3A credentials
        if self.environment in ("minio", "s3", "aws"):
            configs.update(
                {
                    "spark.hadoop.fs.s3a.endpoint": self.s3.endpoint_url,
                    "spark.hadoop.fs.s3a.access.key": self.s3.access_key,
                    "spark.hadoop.fs.s3a.secret.key": self.s3.secret_key,
                    "spark.hadoop.fs.s3a.path.style.access": str(
                        self.s3.path_style_access
                    ).lower(),
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
                        self.s3.ssl_enabled
                    ).lower(),
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.aws.credentials.provider": (
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                    ),
                }
            )

        return configs


def get_default_config() -> LakehouseConfig:
    """Factory function to instantiate default lakehouse configuration."""
    return LakehouseConfig()
