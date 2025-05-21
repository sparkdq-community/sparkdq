from typing import Literal

from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config

FreshnessPeriod = Literal["year", "month", "week", "day", "hour", "minute", "second"]


class FreshnessCheck(BaseAggregateCheck):
    """
    Aggregate-level check that verifies whether the most recent timestamp in the given column
    is within the allowed freshness threshold relative to the current system time.

    A row fails if the most recent timestamp is older than the configured interval.
    """

    def __init__(
        self,
        check_id: str,
        column: str,
        period: FreshnessPeriod,
        interval: int,
        severity: Severity = Severity.CRITICAL,
    ):
        super().__init__(check_id=check_id, severity=severity)
        self.column = column
        self.period = period
        self.interval = interval

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Evaluates the freshness of the most recent timestamp in a specified column.

        This method checks whether the latest timestamp value in the target column
        falls within a defined freshness threshold relative to the current system time.
        The threshold is defined by a combination of `interval` and `period`
        (e.g., "10 minutes", "2 days").

        The check is performed using a Spark SQL expression that compares the
        most recent timestamp (`max(column)`) against `current_timestamp() - INTERVAL`.

        Args:
            df (DataFrame): The input DataFrame containing the column to validate.

        Returns:
            AggregateEvaluationResult: An object containing the result of the freshness check
            (`passed`) and associated metrics:
                - "max_timestamp": the most recent value in the timestamp column
                - "freshness_threshold": the threshold used for comparison

        Note:
            If the column contains only null values, the check fails by default
            and "max_timestamp" is set to None.
        """
        # Get most recent timestamp
        max_ts = df.select(F.max(F.col(self.column)).alias("max_ts")).first()["max_ts"]  # type: ignore

        if max_ts is None:
            return AggregateEvaluationResult(
                passed=False,
                metrics={
                    "max_timestamp": None,
                    "freshness_threshold": f"{self.interval} {self.period}",
                },
            )

        # Construct comparison expression
        interval_expr = F.expr(f"INTERVAL {self.interval} {self.period.upper()}")
        condition = F.lit(max_ts) >= (F.current_timestamp() - interval_expr)

        # Evaluate condition using a single-row DataFrame
        result = df.sparkSession.range(1).select(condition.alias("freshness_passed")).first()
        passed = result["freshness_passed"]  # type: ignore

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "max_timestamp": str(max_ts),
                "freshness_threshold": f"{self.interval} {self.period}",
            },
        )


@register_check_config(check_name="freshness-check")
class FreshnessCheckConfig(BaseAggregateCheckConfig):
    """
    Declarative configuration model for the FreshnessCheck.

    Ensures that the newest value in the specified timestamp column is recent enough
    relative to the current time.

    Attributes:
        column (str): Name of the timestamp column.
        interval (int): Time window size (must be positive).
        period (str): Unit of time (e.g., "days", "hours", "mins").
    """

    check_class = FreshnessCheck

    column: str = Field(..., description="The timestamp column to check for freshness")
    interval: int = Field(..., description="Number of time units representing allowed delay")
    period: FreshnessPeriod = Field(..., description="Time unit for the freshness interval")

    @model_validator(mode="after")
    def validate_interval(self) -> "FreshnessCheckConfig":
        if self.interval <= 0:
            raise InvalidCheckConfigurationError("interval must be a positive integer")
        return self
