"""Data transformation module for Cloud Data Lakehouse Pipeline.

Author: Prabhat Sharma (prabhat84sharma-ui)
Description:
    Implements Medallion architecture transformations:
    - Bronze to Silver: Data hygiene, deduplication, currency standardization,
      quarantine isolation, and data quality validation.
    - Silver to Gold: Window analytics (cumulative spend, velocity lag, ranking),
      Customer 360 mart, and Regional Daily Sales mart.
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from src.config import LakehouseConfig, get_default_config
from src.utils import DataQualityReport, setup_logger, timer


class DataTransformer:
    """Production PySpark transformation engine for Medallion architecture."""

    def __init__(
        self,
        config: Optional[LakehouseConfig] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize DataTransformer with business configurations."""
        self.config = config or get_default_config()
        self.logger = logger or setup_logger(self.__class__.__name__)

    # =========================================================================
    # BRONZE -> SILVER TRANSFORMATIONS
    # =========================================================================

    def clean_raw_data(self, df: DataFrame) -> DataFrame:
        """Standardize column casing, trim whitespace, and cast core data types.

        Args:
            df: Raw Bronze PySpark DataFrame.

        Returns:
            Cleaned and normalized DataFrame.
        """
        self.logger.info("Executing Bronze data hygiene and canonical type casting...")

        cleaned_df = (
            df.withColumn("transaction_id", F.trim(F.upper(F.col("transaction_id"))))
            .withColumn("customer_id", F.trim(F.upper(F.col("customer_id"))))
            .withColumn("product_id", F.trim(F.upper(F.col("product_id"))))
            .withColumn("amount", F.col("amount").cast(DoubleType()))
            .withColumn("currency", F.trim(F.upper(F.col("currency"))))
            .withColumn(
                "transaction_timestamp",
                F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
            )
            .withColumn("transaction_date", F.to_date(F.col("transaction_timestamp")))
            .withColumn("region", F.trim(F.upper(F.col("region"))))
            .withColumn("status", F.trim(F.upper(F.col("status"))))
        )

        return cleaned_df

    def deduplicate_records(
        self,
        df: DataFrame,
        primary_key: str = "transaction_id",
        ordering_col: str = "transaction_timestamp",
    ) -> DataFrame:
        """Deduplicate records by primary key, retaining the most recent record.

        Args:
            df: Cleaned PySpark DataFrame.
            primary_key: Identifying column for uniqueness.
            ordering_col: Timestamp column to determine latest record.

        Returns:
            Deduplicated DataFrame.
        """
        self.logger.info(
            "Executing stateful deduplication on [%s] ordered by [%s] DESC...",
            primary_key,
            ordering_col,
        )

        initial_count = df.count()
        window_spec = Window.partitionBy(primary_key).orderBy(
            F.col(ordering_col).desc_nulls_last()
        )

        deduped_df = (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        deduped_count = deduped_df.count()
        dropped = initial_count - deduped_count
        self.logger.info(
            "Deduplication completed: %d records retained, %d duplicate records purged.",
            deduped_count,
            dropped,
        )

        return deduped_df

    def enrich_currency_and_metrics(self, df: DataFrame) -> DataFrame:
        """Normalize transaction amounts to USD and compute risk/tier flags.

        Args:
            df: Deduplicated PySpark DataFrame.

        Returns:
            Enriched DataFrame with amount_usd and high-value flags.
        """
        self.logger.info("Applying FX normalization and business risk scoring...")

        rates = self.config.business.exchange_rates_to_usd
        # Build Spark conditional expression for currency mapping
        fx_expr = F.lit(1.0)
        for curr, rate in rates.items():
            fx_expr = F.when(F.col("currency") == curr, F.lit(rate)).otherwise(fx_expr)

        enriched_df = (
            df.withColumn("fx_rate_to_usd", fx_expr)
            .withColumn(
                "amount_usd",
                F.round(F.col("amount") * F.col("fx_rate_to_usd"), 2),
            )
            .withColumn(
                "is_high_value",
                F.col("amount_usd") >= self.config.business.high_value_threshold_usd,
            )
            .withColumn(
                "customer_tier",
                F.when(F.col("amount_usd") >= 1000.0, "VIP")
                .when(F.col("amount_usd") >= 300.0, "PREMIUM")
                .otherwise("STANDARD"),
            )
            .withColumn("_processed_timestamp", F.current_timestamp())
        )

        return enriched_df

    def validate_quality_and_split(
        self, df: DataFrame
    ) -> Tuple[DataFrame, DataFrame, DataQualityReport]:
        """Validate data quality rules and partition into valid vs quarantine records.

        Args:
            df: Enriched PySpark DataFrame.

        Returns:
            Tuple of (valid_records_df, quarantine_records_df, quality_report).
        """
        self.logger.info("Evaluating enterprise Data Quality rules...")
        report = DataQualityReport(stage="SilverValidation")
        total_records = df.count()

        # Rule 1: Primary key non-null and non-empty
        pk_condition = F.col("transaction_id").isNotNull() & (F.length("transaction_id") > 0)
        pk_pass = df.filter(pk_condition).count()
        report.add_check(
            rule_name="Non_Null_Transaction_ID",
            description="Transaction ID must be non-null and non-empty",
            total=total_records,
            passed=pk_pass,
            failed=total_records - pk_pass,
            threshold_pct=100.0,
        )

        # Rule 2: Positive transaction amount
        amount_condition = F.col("amount").isNotNull() & (F.col("amount") > 0.0)
        amount_pass = df.filter(amount_condition).count()
        report.add_check(
            rule_name="Positive_Amount",
            description="Transaction amount must be strictly positive",
            total=total_records,
            passed=amount_pass,
            failed=total_records - amount_pass,
            threshold_pct=95.0,
        )

        # Rule 3: Valid status whitelist
        valid_statuses = list(self.config.business.valid_statuses)
        status_condition = F.col("status").isin(valid_statuses)
        status_pass = df.filter(status_condition).count()
        report.add_check(
            rule_name="Valid_Status_Enum",
            description="Status must match defined lifecycle whitelist",
            total=total_records,
            passed=status_pass,
            failed=total_records - status_pass,
            threshold_pct=95.0,
        )

        # Rule 4: Valid parseable timestamp
        ts_condition = F.col("transaction_timestamp").isNotNull()
        ts_pass = df.filter(ts_condition).count()
        report.add_check(
            rule_name="Valid_Timestamp",
            description="Timestamp must conform to standard ISO/SQL timestamp",
            total=total_records,
            passed=ts_pass,
            failed=total_records - ts_pass,
            threshold_pct=98.0,
        )

        # Master filter condition for Silver promotion
        composite_condition = (
            pk_condition & amount_condition & status_condition & ts_condition
        )

        valid_df = df.filter(composite_condition)
        quarantine_df = df.filter(~composite_condition).withColumn(
            "_quarantine_reason",
            F.concat_ws(
                "; ",
                F.when(~pk_condition, F.lit("Invalid Transaction ID")),
                F.when(~amount_condition, F.lit("Negative or Null Amount")),
                F.when(~status_condition, F.lit("Unknown Status")),
                F.when(~ts_condition, F.lit("Unparseable Timestamp")),
            ),
        )

        report.print_summary(self.logger)
        self.logger.info(
            "Quality gate completed: %d valid records promoted to Silver; %d records quarantined.",
            valid_df.count(),
            quarantine_df.count(),
        )

        return valid_df, quarantine_df, report

    def transform_to_silver(
        self, bronze_df: DataFrame
    ) -> Tuple[DataFrame, DataFrame, DataQualityReport]:
        """Execute full end-to-end Bronze to Silver pipeline stage."""
        with timer("Bronze to Silver Transformation", self.logger):
            cleaned = self.clean_raw_data(bronze_df)
            deduped = self.deduplicate_records(cleaned)
            enriched = self.enrich_currency_and_metrics(deduped)
            valid_df, quarantine_df, report = self.validate_quality_and_split(enriched)
            return valid_df, quarantine_df, report

    # =========================================================================
    # SILVER -> GOLD WINDOW ANALYTICS & AGGREGATIONS
    # =========================================================================

    def apply_window_analytics(self, silver_df: DataFrame) -> DataFrame:
        """Apply advanced PySpark window functions for customer sequence, lag, and running totals.

        Calculates:
        - Customer transaction sequence number
        - Previous transaction timestamp and interval in minutes (LAG)
        - Cumulative customer lifetime spend (running sum)
        - Rolling 30-day customer spend
        """
        self.logger.info("Computing analytical Window functions across Silver dataset...")

        customer_time_window = Window.partitionBy("customer_id").orderBy(
            F.col("transaction_timestamp")
        )

        # Rolling 30-day range window (30 days in seconds = 2,592,000)
        thirty_days_sec = 30 * 24 * 60 * 60
        rolling_30d_window = (
            Window.partitionBy("customer_id")
            .orderBy(F.col("transaction_timestamp").cast("long"))
            .rangeBetween(-thirty_days_sec, 0)
        )

        windowed_df = (
            silver_df
            # 1. Customer transaction sequence
            .withColumn(
                "customer_transaction_seq",
                F.row_number().over(customer_time_window),
            )
            # 2. Previous transaction timestamp and time delta (LAG)
            .withColumn(
                "prev_transaction_timestamp",
                F.lag("transaction_timestamp", 1).over(customer_time_window),
            )
            .withColumn(
                "minutes_since_last_transaction",
                F.round(
                    (
                        F.col("transaction_timestamp").cast("long")
                        - F.col("prev_transaction_timestamp").cast("long")
                    )
                    / 60.0,
                    2,
                ),
            )
            # 3. Running cumulative spend per customer
            .withColumn(
                "cumulative_customer_spend_usd",
                F.round(
                    F.sum("amount_usd").over(
                        customer_time_window.rowsBetween(
                            Window.unboundedPreceding, Window.currentRow
                        )
                    ),
                    2,
                ),
            )
            # 4. Rolling 30-day spend
            .withColumn(
                "rolling_30d_customer_spend_usd",
                F.round(F.sum("amount_usd").over(rolling_30d_window), 2),
            )
            # 5. Velocity anomaly flag (transactions < 5 mins apart)
            .withColumn(
                "is_velocity_alert",
                (F.col("minutes_since_last_transaction").isNotNull())
                & (F.col("minutes_since_last_transaction") < 5.0),
            )
        )

        return windowed_df

    def build_customer_360_mart(self, silver_df: DataFrame) -> DataFrame:
        """Construct Customer 360 analytical gold data mart.

        Aggregates customer lifetime spend, transaction frequency, RFM metrics,
        and derives customer value rank using DENSE_RANK window function.
        """
        self.logger.info("Constructing Gold Mart: [Customer 360]...")

        agg_df = silver_df.groupBy("customer_id").agg(
            F.count("transaction_id").alias("total_transactions"),
            F.count(F.when(F.col("status") == "COMPLETED", True)).alias(
                "completed_transactions"
            ),
            F.count(F.when(F.col("status") == "REFUNDED", True)).alias(
                "refunded_transactions"
            ),
            F.round(
                F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount_usd")).otherwise(0.0)),
                2,
            ).alias("total_net_spend_usd"),
            F.round(
                F.avg(F.when(F.col("status") == "COMPLETED", F.col("amount_usd")).otherwise(0.0)),
                2,
            ).alias("avg_order_value_usd"),
            F.min("transaction_timestamp").alias("first_transaction_at"),
            F.max("transaction_timestamp").alias("last_transaction_at"),
            # Most frequent region (mode approximation via max by frequency)
            F.first("region").alias("primary_region"),
        )

        # Compute Customer Lifetime Value (LTV) rank
        ltv_window = Window.orderBy(F.col("total_net_spend_usd").desc())

        customer_360 = (
            agg_df.withColumn("ltv_rank", F.dense_rank().over(ltv_window))
            .withColumn(
                "customer_segment",
                F.when(F.col("total_net_spend_usd") >= 5000.0, "ENTERPRISE_VIP")
                .when(F.col("total_net_spend_usd") >= 1000.0, "HIGH_VALUE")
                .when(F.col("total_net_spend_usd") >= 300.0, "GROWTH")
                .otherwise("CASUAL"),
            )
            .withColumn("_mart_refreshed_at", F.current_timestamp())
        )

        return customer_360

    def build_regional_sales_mart(self, silver_df: DataFrame) -> DataFrame:
        """Construct Regional Daily Sales performance gold data mart."""
        self.logger.info("Constructing Gold Mart: [Regional Daily Sales]...")

        regional_mart = (
            silver_df.groupBy("region", "transaction_date")
            .agg(
                F.count("transaction_id").alias("total_orders"),
                F.count(F.when(F.col("status") == "COMPLETED", True)).alias(
                    "completed_orders"
                ),
                F.count(F.when(F.col("status") == "REFUNDED", True)).alias(
                    "refunded_orders"
                ),
                F.count(F.when(F.col("status") == "FAILED", True)).alias(
                    "failed_orders"
                ),
                F.round(
                    F.sum(
                        F.when(F.col("status") == "COMPLETED", F.col("amount_usd")).otherwise(0.0)
                    ),
                    2,
                ).alias("gross_revenue_usd"),
                F.round(
                    F.avg(
                        F.when(F.col("status") == "COMPLETED", F.col("amount_usd")).otherwise(0.0)
                    ),
                    2,
                ).alias("avg_order_value_usd"),
            )
            .withColumn(
                "refund_rate_pct",
                F.round(
                    (F.col("refunded_orders") / F.col("total_orders")) * 100.0, 2
                ),
            )
            .withColumn(
                "failure_rate_pct",
                F.round(
                    (F.col("failed_orders") / F.col("total_orders")) * 100.0, 2
                ),
            )
            .withColumn("_mart_refreshed_at", F.current_timestamp())
        )

        return regional_mart
