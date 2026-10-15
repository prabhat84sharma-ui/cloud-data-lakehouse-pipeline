"""Utility functions and helper modules for Cloud Data Lakehouse Pipeline.

Author: Prabhat Sharma (prabhat84sharma-ui)
Description:
    Contains logger configuration, SparkSession factory with Delta Lake 3.x
    extensions, benchmark timing context managers, and structured data quality
    reporting utilities.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.config import LakehouseConfig, get_default_config


def setup_logger(
    name: str = "LakehousePipeline",
    log_level: str = "INFO",
    log_file: Optional[str] = None,
) -> logging.Logger:
    """Set up and configure a structured, formatted logger.

    Args:
        name: Name of the logger instance.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Optional file path to persist log output.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Avoid adding duplicate handlers if logger was already created
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


def get_spark_session(
    config: Optional[LakehouseConfig] = None,
    app_name: Optional[str] = None,
) -> SparkSession:
    """Initialize and return a production-tuned SparkSession with Delta Lake 3.x.

    Args:
        config: LakehouseConfig instance with Spark & Delta settings.
        app_name: Override application name if provided.

    Returns:
        Configured and active SparkSession.
    """
    cfg = config or get_default_config()
    target_app_name = app_name or cfg.spark.app_name

    builder = (
        SparkSession.builder.appName(target_app_name)
        .master(cfg.spark.master)
    )

    # Apply all spark parameters defined in configuration
    for key, val in cfg.get_spark_parameters().items():
        builder = builder.config(key, val)

    # Automatically bundle delta-spark maven dependency if not already on classpath
    try:
        import delta
        builder = delta.configure_spark_with_delta_pip(builder)
    except ImportError:
        pass

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def timer(step_name: str, logger: Optional[logging.Logger] = None) -> Generator[None, None, None]:
    """Context manager to benchmark and log execution duration of pipeline steps."""
    log = logger or setup_logger("LakehouseBenchmark")
    log.info(">> Starting execution step: [%s]", step_name)
    start_time = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start_time
        log.info("<< Completed step: [%s] in %.3f seconds", step_name, elapsed)


@dataclass
class QualityCheckResult:
    """Individual data quality rule check result."""

    rule_name: str
    description: str
    total_records: int
    passed_records: int
    failed_records: int
    pass_rate_pct: float
    status: str  # PASSED or FAILED


@dataclass
class DataQualityReport:
    """Comprehensive summary report for data quality validations."""

    stage: str
    evaluated_at: str = field(
        default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    )
    total_evaluated_records: int = 0
    checks: List[QualityCheckResult] = field(default_factory=list)
    overall_status: str = "PASSED"

    def add_check(
        self,
        rule_name: str,
        description: str,
        total: int,
        passed: int,
        failed: int,
        threshold_pct: float = 95.0,
    ) -> None:
        """Add check outcome and evaluate pass/fail against threshold."""
        pass_rate = (passed / total * 100.0) if total > 0 else 100.0
        status = "PASSED" if pass_rate >= threshold_pct else "FAILED"
        if status == "FAILED":
            self.overall_status = "FAILED"

        self.checks.append(
            QualityCheckResult(
                rule_name=rule_name,
                description=description,
                total_records=total,
                passed_records=passed,
                failed_records=failed,
                pass_rate_pct=round(pass_rate, 2),
                status=status,
            )
        )
        self.total_evaluated_records = total

    def print_summary(self, logger: Optional[logging.Logger] = None) -> None:
        """Print a formatted ASCII summary table of quality metrics."""
        log = logger or setup_logger("DataQualityReport")
        log.info("=" * 80)
        log.info("DATA QUALITY REPORT: [%s] | Overall Status: [%s]", self.stage, self.overall_status)
        log.info("Evaluated at: %s | Total Records: %d", self.evaluated_at, self.total_evaluated_records)
        log.info("-" * 80)
        log.info(
            "%-25s %-12s %-10s %-10s %-10s %s",
            "Rule Name",
            "Status",
            "Total",
            "Passed",
            "Failed",
            "Pass %",
        )
        log.info("-" * 80)
        for check in self.checks:
            log.info(
                "%-25s %-12s %-10d %-10d %-10d %.2f%%",
                check.rule_name[:24],
                check.status,
                check.total_records,
                check.passed_records,
                check.failed_records,
                check.pass_rate_pct,
            )
        log.info("=" * 80)

    def to_json(self, output_path: Optional[str] = None) -> str:
        """Export quality metrics to JSON string and optionally save to disk."""
        data = {
            "stage": self.stage,
            "evaluated_at": self.evaluated_at,
            "overall_status": self.overall_status,
            "total_evaluated_records": self.total_evaluated_records,
            "checks": [asdict(c) for c in self.checks],
        }
        json_str = json.dumps(data, indent=2)
        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(json_str)
        return json_str
