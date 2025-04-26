Count Checks
============


Row-Count-Min-Check
-------------------

.. raw:: html

   <hr>

**Purpose**: Verifies that the input DataFrame contains at least a specified minimum number of rows.
Prevents downstream processing on incomplete or unexpectedly small datasets.

**Type**: Aggregate

**Python Configuration**:

.. code-block:: python

   from sparkdq.checks import RowCountMinCheckConfig
   from sparkdq.core import Severity

   RowCountMinCheckConfig(
       check_id="minimum_required_records",
       min_count=10000,
       severity=Severity.WARNING
   )

**YAML Configuration**:

.. code-block:: yaml

    - check: row-count-min-check
      check_id: minimum_required_records
      min_count: 10000
      severity: warning


Row-Count-Max-Check
-------------------

.. raw:: html

   <hr>

**Purpose**: Verifies that the input DataFrame does not exceed a specified maximum number of rows.
Helps to detect unexpected data growth or duplication early in the pipeline.

**Type**: Aggregate

**Python Configuration**:

.. code-block:: python

    from sparkdq.checks import RowCountMaxCheckConfig
    from sparkdq.core import Severity

    RowCountMaxCheckConfig(
        check_id="prevent_oversize_batch",
        max_count=100000,
        severity=Severity.ERROR
    )

**YAML Configuration**:

.. code-block:: yaml

    - check: row-count-max-check
      check_id: prevent_oversize_batch
      max_count: 100000
      severity: error


Row-Count-Between-Check
-----------------------

.. raw:: html

   <hr>

**Purpose**: Verifies that the number of rows in the dataset falls within a specified minimum and maximum range.
Ensures data completeness while preventing unexpected data volume.

**Type**: Aggregate

**Python Configuration**

.. code-block:: python

   from sparkdq.checks import RowCountBetweenCheckConfig
   from sparkdq.core import Severity

   RowCountBetweenCheckConfig(
       check_id="expected_daily_batch_size",
       min_count=1000,
       max_count=5000,
       severity=Severity.ERROR
   )

**YAML Configuration**

.. code-block:: yaml

    - check: row-count-between-check
      check_id: expected_daily_batch_size
      min_count: 1000
      max_count: 5000
      severity: error


Row-Count-Exact-Check
---------------------

.. raw:: html

   <hr>

**Purpose**: Verifies that the dataset contains exactly the specified number of rows.
Useful for enforcing strict expectations on data volume.

**Type**: Aggregate

**Python Configuration**:

.. code-block:: python

    from sparkdq.checks import RowCountExactCheckConfig
    from sparkdq.core import Severity

    RowCountExactCheckConfig(
        check_id="validate_snapshot_size",
        expected_count=500,
        severity=Severity.ERROR
    )

**YAML Configuration**:

.. code-block:: yaml

    - check: row-count-exact-check
      check_id: validate_snapshot_size
      expected_count: 500
      severity: error
