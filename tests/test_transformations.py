"""Unit tests for PySpark Medallion Transformations.

Author: Prabhat Sharma (prabhat84sharma-ui)
Description:
    Test suite verifying data hygiene, deduplication, currency normalization,
    quality gates, window analytics, and gold aggregation marts.
"""

from __future__ import annotations

import os
from typing import Generator

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.config import LakehouseConfig
from src.transform import DataTransformer


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide a lightweight local SparkSession for test execution."""
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("LakehouseUnitTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="session")
def config() -> LakehouseConfig:
    """Provide default Lakehouse configuration."""
    return LakehouseConfig()


@pytest.fixture(scope="session")
def transformer(config: LakehouseConfig) -> DataTransformer:
    """Provide DataTransformer instance."""
    return DataTransformer(config=config)


@pytest.fixture
def sample_raw_df(spark: SparkSession) -> DataFrame:
    """Fixture providing raw uncleaned transaction records."""
    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("region", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )
    data = [
        (" txn-001 ", " cust-a ", "prod-x", "100.50", " usd ", "2024-03-01 10:00:00", "us-east", "completed"),
        ("TXN-002", "CUST-B", "PROD-Y", "200.00", "EUR", "2024-03-01 11:00:00", "EU-WEST", "COMPLETED"),
        ("TXN-003", "CUST-A", "PROD-Z", "-50.00", "USD", "2024-03-01 12:00:00", "US-EAST", "FAILED"),
        ("TXN-004", "CUST-C", "PROD-W", "1500.00", "USD", "2024-03-01 13:00:00", "US-WEST", "COMPLETED"),
        ("TXN-005", "CUST-B", "PROD-Y", "75.00", "GBP", "2024-03-01 14:00:00", "EU-WEST", "REFUNDED"),
    ]
    return spark.createDataFrame(data, schema)


def test_clean_raw_data(transformer: DataTransformer, sample_raw_df: DataFrame) -> None:
    """Verify whitespace trimming, uppercase standardizing, and type casting."""
    cleaned = transformer.clean_raw_data(sample_raw_df)

    row = cleaned.filter(cleaned.transaction_id == "TXN-001").first()
    assert row is not None
    assert row.transaction_id == "TXN-001"
    assert row.customer_id == "CUST-A"
    assert row.currency == "USD"
    assert row.region == "US-EAST"
    assert row.status == "COMPLETED"
    assert isinstance(row.amount, float)
    assert row.amount == pytest.approx(100.50)
    assert str(row.transaction_date) == "2024-03-01"


def test_deduplicate_records(spark: SparkSession, transformer: DataTransformer) -> None:
    """Verify deduplication retains only the record with latest timestamp."""
    schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("transaction_timestamp", StringType(), False),
            StructField("status", StringType(), False),
        ]
    )
    data = [
        ("TXN-100", "2024-03-01 10:00:00", "PENDING"),
        ("TXN-100", "2024-03-01 10:05:00", "COMPLETED"),  # Newer
        ("TXN-200", "2024-03-01 11:00:00", "COMPLETED"),
    ]
    df = spark.createDataFrame(data, schema)

    deduped = transformer.deduplicate_records(
        df, primary_key="transaction_id", ordering_col="transaction_timestamp"
    )

    assert deduped.count() == 2
    row_100 = deduped.filter(deduped.transaction_id == "TXN-100").first()
    assert row_100.status == "COMPLETED"
    assert row_100.transaction_timestamp == "2024-03-01 10:05:00"


def test_enrich_currency_and_metrics(
    spark: SparkSession, transformer: DataTransformer
) -> None:
    """Verify FX normalization and customer tier assignment."""
    schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), False),
        ]
    )
    data = [
        ("TXN-01", 100.0, "USD"),    # 100 * 1.0 = 100.0 -> STANDARD
        ("TXN-02", 400.0, "USD"),    # 400.0 -> PREMIUM
        ("TXN-03", 1000.0, "EUR"),   # 1000 * 1.092 = 1092.0 -> VIP
        ("TXN-04", 100000.0, "INR"), # 100000 * 0.012 = 1200.0 -> VIP
    ]
    df = spark.createDataFrame(data, schema)
    enriched = transformer.enrich_currency_and_metrics(df)

    rows = {r.transaction_id: r for r in enriched.collect()}

    assert rows["TXN-01"].amount_usd == pytest.approx(100.0)
    assert rows["TXN-01"].customer_tier == "STANDARD"
    assert not rows["TXN-01"].is_high_value

    assert rows["TXN-02"].amount_usd == pytest.approx(400.0)
    assert rows["TXN-02"].customer_tier == "PREMIUM"

    assert rows["TXN-03"].amount_usd == pytest.approx(1092.0)
    assert rows["TXN-03"].customer_tier == "VIP"
    assert rows["TXN-03"].is_high_value

    assert rows["TXN-04"].amount_usd == pytest.approx(1200.0)
    assert rows["TXN-04"].customer_tier == "VIP"


def test_validate_quality_and_split(
    spark: SparkSession, transformer: DataTransformer, sample_raw_df: DataFrame
) -> None:
    """Verify that invalid amounts or missing keys route correctly to quarantine."""
    cleaned = transformer.clean_raw_data(sample_raw_df)
    enriched = transformer.enrich_currency_and_metrics(cleaned)

    valid_df, quarantine_df, report = transformer.validate_quality_and_split(enriched)

    assert valid_df.count() == 4
    assert quarantine_df.count() == 1

    # TXN-003 has negative amount (-50.00)
    bad_record = quarantine_df.first()
    assert bad_record.transaction_id == "TXN-003"
    assert "Negative or Null Amount" in bad_record._quarantine_reason
    assert report.overall_status in ("PASSED", "FAILED")


def test_window_analytics(spark: SparkSession, transformer: DataTransformer) -> None:
    """Verify PySpark window functions (sequence numbering, lag delta, and cumulative spend)."""
    schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount_usd", DoubleType(), False),
            StructField("transaction_timestamp", StringType(), False),
            StructField("status", StringType(), False),
        ]
    )
    data = [
        ("TXN-01", "CUST-1", 100.0, "2024-03-01 10:00:00", "COMPLETED"),
        ("TXN-02", "CUST-1", 50.0, "2024-03-01 10:10:00", "COMPLETED"),   # 10 mins later, running spend: 150
        ("TXN-03", "CUST-1", 200.0, "2024-03-01 10:12:00", "COMPLETED"),  # 2 mins later (velocity alert!)
        ("TXN-04", "CUST-2", 300.0, "2024-03-01 11:00:00", "COMPLETED"),
    ]
    df = spark.createDataFrame(data, schema).withColumn(
        "transaction_timestamp",
        spark.read.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(spark.sparkContext.parallelize(["2024-03-01 10:00:00"]))
        .first()[0],
    )
    # Using explicit to_timestamp
    from pyspark.sql import functions as F
    df = spark.createDataFrame(data, schema).withColumn(
        "transaction_timestamp", F.to_timestamp("transaction_timestamp")
    )

    windowed = transformer.apply_window_analytics(df)
    results = windowed.filter(windowed.customer_id == "CUST-1").orderBy("transaction_timestamp").collect()

    assert len(results) == 3
    # Check sequence
    assert results[0].customer_transaction_seq == 1
    assert results[1].customer_transaction_seq == 2
    assert results[2].customer_transaction_seq == 3

    # Check lag delta (minutes)
    assert results[0].minutes_since_last_transaction is None
    assert results[1].minutes_since_last_transaction == pytest.approx(10.0)
    assert results[2].minutes_since_last_transaction == pytest.approx(2.0)
    assert results[2].is_velocity_alert is True

    # Check cumulative running spend
    assert results[0].cumulative_customer_spend_usd == pytest.approx(100.0)
    assert results[1].cumulative_customer_spend_usd == pytest.approx(150.0)
    assert results[2].cumulative_customer_spend_usd == pytest.approx(350.0)


def test_customer_360_gold_mart(spark: SparkSession, transformer: DataTransformer) -> None:
    """Verify Customer 360 gold aggregation and LTV dense ranking."""
    schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount_usd", DoubleType(), False),
            StructField("status", StringType(), False),
            StructField("transaction_timestamp", StringType(), False),
            StructField("region", StringType(), False),
        ]
    )
    from pyspark.sql import functions as F
    data = [
        ("T1", "CUST-A", 5000.0, "COMPLETED", "2024-03-01 10:00:00", "US-EAST"),
        ("T2", "CUST-B", 200.0, "COMPLETED", "2024-03-01 11:00:00", "EU-WEST"),
        ("T3", "CUST-B", 100.0, "REFUNDED", "2024-03-01 12:00:00", "EU-WEST"),
    ]
    df = spark.createDataFrame(data, schema).withColumn(
        "transaction_timestamp", F.to_timestamp("transaction_timestamp")
    )

    customer_360 = transformer.build_customer_360_mart(df)
    records = {r.customer_id: r for r in customer_360.collect()}

    assert len(records) == 2
    cust_a = records["CUST-A"]
    assert cust_a.ltv_rank == 1
    assert cust_a.total_transactions == 1
    assert cust_a.total_net_spend_usd == pytest.approx(5000.0)
    assert cust_a.customer_segment == "ENTERPRISE_VIP"

    cust_b = records["CUST-B"]
    assert cust_b.ltv_rank == 2
    assert cust_b.total_transactions == 2
    assert cust_b.completed_transactions == 1
    assert cust_b.refunded_transactions == 1
    assert cust_b.total_net_spend_usd == pytest.approx(200.0)
    assert cust_b.customer_segment == "CASUAL"


def test_regional_sales_gold_mart(spark: SparkSession, transformer: DataTransformer) -> None:
    """Verify Regional sales performance aggregations and refund percentage calculations."""
    schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("region", StringType(), False),
            StructField("transaction_date", StringType(), False),
            StructField("amount_usd", DoubleType(), False),
            StructField("status", StringType(), False),
        ]
    )
    from pyspark.sql import functions as F
    data = [
        ("T1", "US-EAST", "2024-03-01", 1000.0, "COMPLETED"),
        ("T2", "US-EAST", "2024-03-01", 200.0, "REFUNDED"),
        ("T3", "US-EAST", "2024-03-01", 50.0, "FAILED"),
    ]
    df = spark.createDataFrame(data, schema).withColumn(
        "transaction_date", F.to_date("transaction_date")
    )

    regional_mart = transformer.build_regional_sales_mart(df)
    record = regional_mart.first()

    assert record.region == "US-EAST"
    assert record.total_orders == 3
    assert record.completed_orders == 1
    assert record.refunded_orders == 1
    assert record.failed_orders == 1
    assert record.gross_revenue_usd == pytest.approx(1000.0)
    assert record.refund_rate_pct == pytest.approx(33.33, abs=0.01)
    assert record.failure_rate_pct == pytest.approx(33.33, abs=0.01)
