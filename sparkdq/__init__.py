__version__ = "0.11.0"

# Ensure PySpark is available
import importlib.util

if importlib.util.find_spec("pyspark") is None:
    raise ImportError(
        "PySpark not found. Install with: pip install sparkdq[spark] "
        "or ensure PySpark is available in your environment."
    )
