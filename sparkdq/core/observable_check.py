"""
Observable aggregate check interface for single-pass execution.

This module defines the ObservableAggregateCheck mixin that allows aggregate checks
to declare their Spark aggregation expressions upfront. The BatchCheckRunner can then
collect all expressions and execute them in a single df.agg() call instead of
triggering one Spark job per check.

Checks that cannot be expressed as a single agg() call (e.g. groupBy, join, distinct)
remain as standard BaseAggregateCheck implementations and fall back to individual
execution automatically.
"""

from abc import abstractmethod
from typing import Any

from pyspark.sql import Column

from sparkdq.core.check_results import AggregateEvaluationResult


class ObservableAggregateCheck:
    """
    Mixin for aggregate checks that can participate in a single-pass agg() execution.

    When a BatchCheckRunner detects checks implementing this mixin, it collects all
    their aggregation expressions and executes them in a single df.agg() call.
    Results are then distributed back to each check via _evaluate_from_agg_results().

    To implement this mixin, a check must:
    1. Inherit from both BaseAggregateCheck and ObservableAggregateCheck.
    2. Implement aggregations() to return a dict of {metric_name: Column expression}.
    3. Implement _evaluate_from_agg_results() to compute pass/fail from the resolved values.
    4. Keep _evaluate_logic() as a fallback (call _evaluate_from_agg_results with manual values).

    The metric_name keys in aggregations() are scoped per check instance via the check_id,
    so duplicate names across different checks do not collide.

    Example::

        class MyCheck(BaseAggregateCheck, ObservableAggregateCheck):
            def aggregations(self) -> dict[str, Column]:
                return {"total": F.count("*")}

            def _evaluate_from_agg_results(self, results: dict[str, Any]) -> AggregateEvaluationResult:
                total = results["total"]
                return AggregateEvaluationResult(passed=total > 0, metrics={"total": total})

            def _evaluate_logic(self, df) -> AggregateEvaluationResult:
                row = df.agg(*[v.alias(k) for k, v in self.aggregations().items()]).first()
                return self._evaluate_from_agg_results(dict(row.asDict()))
    """

    @abstractmethod
    def aggregations(self) -> dict[str, Column]:
        """
        Declare the Spark aggregation expressions needed by this check.

        Keys must be unique within a single check instance. The runner prefixes
        them with the check_id to avoid collisions across checks.

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
