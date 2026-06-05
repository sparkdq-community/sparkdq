"""
Observable aggregate check ABC for single-pass execution.

This module defines ObservableAggregateCheck, an abstract base class for aggregate
checks that can declare their Spark aggregation expressions upfront. The BatchCheckRunner
collects all expressions and executes them in a single df.agg() call instead of
triggering one Spark job per check.

Checks that cannot be expressed as a single agg() call (e.g. groupBy, join, distinct)
remain as standard BaseAggregateCheck implementations and fall back to individual
execution automatically.
"""

from abc import abstractmethod
from typing import Any

from pyspark.sql import Column, DataFrame

from sparkdq.core.base_check import BaseAggregateCheck
from sparkdq.core.check_results import AggregateEvaluationResult


class ObservableAggregateCheck(BaseAggregateCheck):
    """
    Abstract base class for aggregate checks that participate in single-pass agg() execution.

    When a BatchCheckRunner detects checks implementing this ABC, it collects all
    their aggregation expressions and executes them in a single df.agg() call.
    Results are then distributed back to each check via _evaluate_from_agg_results().

    Subclasses must implement:
    - aggregations(): return a dict of {metric_name: Column expression}.
    - _evaluate_from_agg_results(): compute pass/fail from the resolved values.

    The metric_name keys in aggregations() are scoped per check instance by the runner,
    so duplicate names across different checks do not collide.

    Example::

        class MyCheck(ObservableAggregateCheck):
            def aggregations(self) -> dict[str, Column]:
                return {"total": F.count("*")}

            def _evaluate_from_agg_results(self, results: dict[str, Any]) -> AggregateEvaluationResult:
                total = results["total"]
                return AggregateEvaluationResult(passed=total > 0, metrics={"total": total})
    """

    @abstractmethod
    def aggregations(self) -> dict[str, Column]:
        """
        Declare the Spark aggregation expressions needed by this check.

        Keys must be unique within a single check instance. The runner scopes
        them by check index to avoid collisions across checks.

        Returns:
            dict[str, Column]: Mapping of metric name to Spark Column expression.
        """
        ...

    @abstractmethod
    def _evaluate_from_agg_results(self, results: dict[str, Any]) -> AggregateEvaluationResult:
        """
        Evaluate the check result from pre-computed aggregation values.

        Args:
            results (dict[str, Any]): Resolved metric values keyed by the names
                returned from aggregations(). Types depend on the Spark aggregation
                (e.g. int for count, float for avg, datetime for max timestamp).

        Returns:
            AggregateEvaluationResult: Check outcome with pass/fail status and metrics.
        """
        ...

    def _evaluate_logic(self, df: DataFrame) -> AggregateEvaluationResult:
        """
        Default implementation that runs aggregations() in a single df.agg() call.

        Subclasses do not need to override this method unless they require custom
        pre-processing before aggregation (e.g. column existence validation).
        The BatchCheckRunner bypasses _evaluate_logic() entirely for observable checks
        and calls _evaluate_from_agg_results() directly with batched results.

        Args:
            df (DataFrame): The dataset to evaluate.

        Returns:
            AggregateEvaluationResult: Check outcome derived from the aggregation results.
        """
        row = df.agg(*[expr.alias(k) for k, expr in self.aggregations().items()]).first()
        return self._evaluate_from_agg_results(row.asDict())  # type: ignore[union-attr]
