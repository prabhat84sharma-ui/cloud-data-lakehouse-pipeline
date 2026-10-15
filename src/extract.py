"""Data extraction module for Cloud Data Lakehouse Pipeline.

Author: Prabhat Sharma (prabhat84sharma-ui)
Description:
    Handles multi-format ingestion (CSV, JSON, Parquet, Delta) with schema
    validation, lineage metadata tracking, and automatic data profiling.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import LakehouseConfig, get_default_config
from src.utils import setup_logger, timer


class DataExtractor:
    """Production-grade multi-source data extractor for lakehouse pipelines."""

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[LakehouseConfig] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize DataExtractor with active Spark session and configurations."""
        self.spark = spark
        self.config = config or get_default_config()
        self.logger = logger or setup_logger(self.__class__.__name__)

    @staticmethod
    def get_raw_transactions_schema() -> StructType:
        """Explicit schema definition for raw incoming transaction streams."""
        return StructType(
            [
                StructField("transaction_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("region", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )

    def extract(
        self,
        file_path: str,
        file_format: str = "csv",
        schema: Optional[StructType] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        """Extract dataset from storage path, apply format options and inject lineage metadata.

        Args:
            file_path: Path or URI (S3/local) of source file(s).
            file_format: Input format ('csv', 'json', 'parquet', 'delta').
            schema: Optional explicit StructType schema.
            options: Additional format options for DataFrameReader.

        Returns:
            PySpark DataFrame with audit metadata columns.
        """
        fmt = file_format.lower().strip()
        opts = options or {}
        batch_id = str(uuid.uuid4())[:8]

        self.logger.info(
            "Initiating ingestion from [%s] | Format: [%s] | Batch ID: [%s]",
            file_path,
            fmt,
            batch_id,
        )

        with timer(f"Extract {fmt.upper()} from {os.path.basename(file_path)}", self.logger):
            reader = self.spark.read

            if schema:
                reader = reader.schema(schema)

            if fmt == "csv":
                reader = (
                    reader.option("header", opts.get("header", "true"))
                    .option("inferSchema", opts.get("inferSchema", "false" if schema else "true"))
                    .option("mode", opts.get("mode", "PERMISSIVE"))
                    .option("ignoreLeadingWhiteSpace", "true")
                    .option("ignoreTrailingWhiteSpace", "true")
                )
                df = reader.csv(file_path)

            elif fmt == "json":
                reader = reader.option("multiline", opts.get("multiline", "false")).option(
                    "mode", opts.get("mode", "PERMISSIVE")
                )
                df = reader.json(file_path)

            elif fmt == "parquet":
                df = reader.parquet(file_path)

            elif fmt == "delta":
                df = reader.format("delta").load(file_path)

            else:
                raise ValueError(
                    f"Unsupported file format '{file_format}'. Supported formats: csv, json, parquet, delta"
                )

            # Inject Data Lakehouse lineage & audit tracking metadata
            df_enriched = (
                df.withColumn("_ingestion_timestamp", F.current_timestamp())
                .withColumn("_source_file", F.input_file_name())
                .withColumn("_batch_id", F.lit(batch_id))
            )

            record_count = df_enriched.count()
            self.logger.info(
                "Ingestion successful: Extracted %d records with %d columns (including audit metadata).",
                record_count,
                len(df_enriched.columns),
            )

            return df_enriched

    def profile_dataset(self, df: DataFrame, sample_limit: int = 5) -> Dict[str, Any]:
        """Perform data profiling and compute statistical summaries on PySpark DataFrame.

        Args:
            df: DataFrame to profile.
            sample_limit: Number of sample records to preview.

        Returns:
            Dictionary containing profiling metrics and null distribution.
        """
        self.logger.info("Computing automated data profiling metrics...")

        row_count = df.count()
        columns = df.columns
        dtypes = dict(df.dtypes)

        # Single-pass aggregation for null checks across all columns
        null_exprs = [
            F.count(F.when(F.col(c).isNull() | F.isnan(F.col(c)) if dtypes[c] in ("double", "float") else F.col(c).isNull(), c)).alias(c)
            for c in columns
        ]
        null_counts_row = df.select(null_exprs).collect()[0].asDict()

        profile_stats = {
            "total_records": row_count,
            "total_columns": len(columns),
            "columns": columns,
            "data_types": dtypes,
            "null_analysis": {
                col_name: {
                    "null_count": count,
                    "null_percentage": round((count / row_count * 100.0) if row_count > 0 else 0.0, 2),
                }
                for col_name, count in null_counts_row.items()
            },
        }

        # Log detailed profiling summary
        self.logger.info("=" * 70)
        self.logger.info("DATA PROFILING SUMMARY (Total Rows: %d)", row_count)
        self.logger.info("-" * 70)
        self.logger.info("%-25s %-15s %-12s %-10s", "Column", "Type", "Null Count", "Null %")
        self.logger.info("-" * 70)
        for col_name, stats in profile_stats["null_analysis"].items():
            self.logger.info(
                "%-25s %-15s %-12d %.2f%%",
                col_name[:24],
                dtypes.get(col_name, "unknown"),
                stats["null_count"],
                stats["null_percentage"],
            )
        self.logger.info("=" * 70)

        return profile_stats

    def extract_and_profile(
        self,
        file_path: str,
        file_format: str = "csv",
        schema: Optional[StructType] = None,
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """Convenience method to extract data and immediately run profiling."""
        df = self.extract(file_path=file_path, file_format=file_format, schema=schema)
        stats = self.profile_dataset(df)
        return df, stats
