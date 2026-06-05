from typing import Any, Literal

from pydantic import Field, model_validator
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.base_config import BaseAggregateCheckConfig
from sparkdq.core.check_results import AggregateEvaluationResult
from sparkdq.core.observable_check import ObservableAggregateCheck
from sparkdq.core.severity import Severity
from sparkdq.exceptions import InvalidCheckConfigurationError
from sparkdq.plugin.check_config_registry import register_check_config

FreshnessPeriod = Literal["year", "month", "week", "day", "hour", "minute", "second"]


class FreshnessCheck(BaseAggregateCheck, ObservableAggregateCheck):
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

    def aggregations(self) -> dict[str, Column]:
        return {"max_ts": F.max(F.col(self.column))}

    def _evaluate_from_agg_results(self, results: dict[str, Any]) -> AggregateEvaluationResult:
        max_ts = results["max_ts"]
        threshold_label = f"{self.interval} {self.period}"

        if max_ts is None:
            return AggregateEvaluationResult(
                passed=False,
                metrics={
                    "max_timestamp": None,
                    "freshness_threshold": threshold_label,
                },
            )

        interval_expr = F.expr(f"INTERVAL {self.interval} {self.period.upper()}")
        condition = F.lit(max_ts) >= (F.current_timestamp() - interval_expr)
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession found for FreshnessCheck evaluation")
        result_row = spark.range(1).select(condition.alias("freshness_passed")).first()
        passed = result_row["freshness_passed"]  # type: ignore[index]

        return AggregateEvaluationResult(
            passed=passed,
            metrics={
                "max_timestamp": str(max_ts),
                "freshness_threshold": threshold_label,
            },
        )

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        row = df.agg(*[expr.alias(k) for k, expr in self.aggregations().items()]).first()
        return self._evaluate_from_agg_results(row.asDict())  # type: ignore[union-attr]


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
