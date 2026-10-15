"""Master Orchestration Pipeline for Cloud Data Lakehouse.

Author: Prabhat Sharma (prabhat84sharma-ui)
Description:
    End-to-end Medallion architecture execution driver:
    Extract (Multi-Source) -> Bronze -> Silver (Clean, Dedup, Quality Gate, Upsert)
    -> Gold (Window Analytics, Customer 360, Regional Marts) -> Maintenance (OPTIMIZE/VACUUM).
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Optional

from src.config import LakehouseConfig, get_default_config
from src.extract import DataExtractor
from src.load import DeltaLakeLoader
from src.transform import DataTransformer
from src.utils import get_spark_session, setup_logger, timer


BANNER = r"""
================================================================================
  CLOUD DATA LAKEHOUSE PIPELINE (Medallion Architecture)
  Author: Prabhat Sharma (https://github.com/prabhat84sharma-ui)
  Engine: Apache Spark 3.5 | Delta Lake 3.2 | Storage: AWS S3 / MinIO
================================================================================
"""


class LakehousePipeline:
    """Orchestrates end-to-end Medallion Lakehouse ETL lifecycle."""

    def __init__(
        self,
        config: Optional[LakehouseConfig] = None,
        logger_name: str = "LakehouseOrchestrator",
    ) -> None:
        """Initialize pipeline with configuration and logging."""
        self.config = config or get_default_config()
        self.logger = setup_logger(logger_name, log_level=self.config.spark.log_level)
        self.spark = None

    def run(
        self,
        source_path: Optional[str] = None,
        stage: str = "all",
        run_optimize: bool = False,
        quality_report_path: Optional[str] = None,
    ) -> int:
        """Execute the lakehouse pipeline stages.

        Args:
            source_path: Override path to input raw data file.
            stage: Target execution stage ('all', 'bronze', 'silver', 'gold').
            run_optimize: Whether to run Delta table compaction and vacuum.
            quality_report_path: Optional path to save JSON data quality report.

        Returns:
            0 on success, 1 on failure.
        """
        print(BANNER)
        self.logger.info("Initializing Cloud Data Lakehouse Pipeline...")
        self.logger.info("Target Environment: [%s] | Execution Stage: [%s]", self.config.environment, stage)

        pipeline_start = time.perf_counter()

        try:
            # 1. Initialize Spark Session
            self.spark = get_spark_session(self.config)
            self.logger.info("SparkSession successfully provisioned. Version: %s", self.spark.version)

            # 2. Instantiate pipeline modules
            extractor = DataExtractor(spark=self.spark, config=self.config, logger=self.logger)
            transformer = DataTransformer(config=self.config, logger=self.logger)
            loader = DeltaLakeLoader(spark=self.spark, config=self.config, logger=self.logger)

            input_path = source_path or self.config.storage.raw_input_path

            # =================================================================
            # STAGE 1: INGESTION & BRONZE LOAD
            # =================================================================
            if stage in ("all", "bronze"):
                with timer("STAGE 1: Ingestion & Bronze Write", self.logger):
                    raw_df, profile_stats = extractor.extract_and_profile(
                        file_path=input_path,
                        file_format="csv",
                    )
                    loader.write_bronze(
                        df=raw_df,
                        target_path=self.config.storage.bronze_path,
                        mode="append",
                    )

            if stage == "bronze":
                self.logger.info("Bronze stage completed successfully.")
                return 0

            # =================================================================
            # STAGE 2: BRONZE -> SILVER (CLEAN, DEDUP, QUALITY GATE, MERGE)
            # =================================================================
            if stage in ("all", "silver", "gold"):
                with timer("STAGE 2: Bronze to Silver Transformation", self.logger):
                    # Read latest from Bronze layer
                    bronze_df = self.spark.read.format("delta").load(
                        self.config.storage.bronze_path
                    )
                    self.logger.info("Loaded %d records from Bronze layer.", bronze_df.count())

                    # Execute data hygiene, deduplication, FX normalization, and quality checks
                    valid_silver_df, quarantine_df, quality_report = transformer.transform_to_silver(
                        bronze_df
                    )

                    # Export quality report if path requested
                    if quality_report_path:
                        quality_report.to_json(quality_report_path)
                        self.logger.info("Quality report saved to: %s", quality_report_path)

                    # Write quarantined invalid records to dead-letter queue
                    loader.write_quarantine(
                        df=quarantine_df,
                        target_path=self.config.storage.quarantine_path,
                    )

                    # Upsert clean data into Silver table with Delta MERGE
                    loader.upsert_silver(
                        df=valid_silver_df,
                        target_path=self.config.storage.silver_path,
                        merge_key="transaction_id",
                        partition_cols=["region"],
                    )

            if stage == "silver":
                self.logger.info("Silver stage completed successfully.")
                return 0

            # =================================================================
            # STAGE 3: SILVER -> GOLD (WINDOW ANALYTICS & AGGREGATED MARTS)
            # =================================================================
            if stage in ("all", "gold"):
                with timer("STAGE 3: Silver to Gold Analytics & Marts", self.logger):
                    silver_df = self.spark.read.format("delta").load(
                        self.config.storage.silver_path
                    )
                    self.logger.info("Loaded %d clean records from Silver layer.", silver_df.count())

                    # 1. Apply Window Analytics
                    windowed_df = transformer.apply_window_analytics(silver_df)
                    self.logger.info("Previewing sample windowed records:")
                    windowed_df.select(
                        "transaction_id",
                        "customer_id",
                        "amount_usd",
                        "customer_transaction_seq",
                        "minutes_since_last_transaction",
                        "cumulative_customer_spend_usd",
                    ).show(5, truncate=False)

                    # 2. Build Customer 360 Mart
                    customer_360_df = transformer.build_customer_360_mart(silver_df)
                    loader.write_gold(
                        df=customer_360_df,
                        target_path=self.config.storage.gold_customer_mart_path,
                        mode="overwrite",
                    )
                    self.logger.info("Top Customers by LTV Rank:")
                    customer_360_df.select(
                        "ltv_rank",
                        "customer_id",
                        "total_transactions",
                        "total_net_spend_usd",
                        "customer_segment",
                    ).orderBy("ltv_rank").show(5, truncate=False)

                    # 3. Build Regional Sales Mart
                    regional_mart_df = transformer.build_regional_sales_mart(silver_df)
                    loader.write_gold(
                        df=regional_mart_df,
                        target_path=self.config.storage.gold_regional_mart_path,
                        mode="overwrite",
                        partition_cols=["region"],
                    )
                    self.logger.info("Regional Sales Performance Preview:")
                    regional_mart_df.show(5, truncate=False)

            # =================================================================
            # STAGE 4: DELTA LAKE MAINTENANCE (OPTIMIZE & VACUUM)
            # =================================================================
            if run_optimize:
                with timer("STAGE 4: Table Compaction & Maintenance", self.logger):
                    loader.optimize_table(
                        table_path=self.config.storage.silver_path,
                        zorder_cols=["customer_id", "transaction_timestamp"],
                    )
                    loader.vacuum_table(
                        table_path=self.config.storage.silver_path,
                        retention_hours=self.config.delta.vacuum_retention_hours,
                    )

            total_elapsed = time.perf_counter() - pipeline_start
            self.logger.info("=" * 80)
            self.logger.info(
                "LAKEHOUSE PIPELINE COMPLETED SUCCESSFULLY IN %.2f SECONDS!", total_elapsed
            )
            self.logger.info("=" * 80)
            return 0

        except Exception as e:
            self.logger.exception("FATAL: Pipeline execution failed with unhandled exception: %s", e)
            return 1

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("SparkSession stopped cleanly.")


def parse_arguments() -> argparse.Namespace:
    """Parse command line flags."""
    parser = argparse.ArgumentParser(
        description="Run Cloud Data Lakehouse Pipeline by Prabhat Sharma"
    )
    parser.add_argument(
        "--stage",
        choices=["all", "bronze", "silver", "gold"],
        default="all",
        help="Pipeline stage to execute (default: all)",
    )
    parser.add_argument(
        "--source-path",
        type=str,
        default=None,
        help="Input raw file path (default: from config)",
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Execute OPTIMIZE with ZORDER and VACUUM maintenance on Delta tables",
    )
    parser.add_argument(
        "--quality-report",
        type=str,
        default="./lakehouse_storage/quality_report.json",
        help="Destination path for Data Quality validation JSON report",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for CLI execution."""
    args = parse_arguments()
    pipeline = LakehousePipeline()
    exit_code = pipeline.run(
        source_path=args.source_path,
        stage=args.stage,
        run_optimize=args.optimize,
        quality_report_path=args.quality_report,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
