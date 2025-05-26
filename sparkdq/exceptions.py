class RuntimeCheckConfigurationError(Exception):
    """
    Base class for runtime configuration errors in data quality checks.

    Raised when a check fails due to a configuration issue that can only
    be detected when the check is executed against actual data, such as
    referencing a non-existent column.
    """


class CheckConfigurationError(Exception):
    """
    Base class for all configuration-related errors in data quality checks.

    Raised when a check's definition or setup is invalid and cannot be used
    to construct or apply the check logic, regardless of the actual dataset.
    """


class MissingColumnError(RuntimeCheckConfigurationError):
    """
    Raised when a required column is not present in the DataFrame at runtime.

    This typically indicates a misconfiguration in the check setup, where a
    column was referenced that does not exist in the current dataset.
    """

    def __init__(self, column: str, available: list[str]):
        super().__init__(f"Column '{column}' not found. Available columns: {available}")


class MissingCheckSetError(RuntimeCheckConfigurationError):
    """
    Raised when a data quality engine is executed without an assigned CheckSet.

    This error indicates that the engine was not properly configured before use.
    Users must assign a CheckSet instance via `engine.set_check_set(...)`
    prior to calling any validation methods like `run_batch`.

    This is typically a programming error and should be caught early in testing.
    """

    def __init__(self) -> None:
        super().__init__(
            "No CheckSet has been assigned to the engine. "
            "Use `engine.set_check_set(check_set)` before running validation."
        )


class InvalidCheckConfigurationError(CheckConfigurationError):
    """
    Raised when a check's configuration is logically invalid.

    Examples include setting a minimum value greater than the maximum, or
    supplying conflicting or incomplete configuration parameters.

    In most cases, this error is raised during static validation of a config object.
    """


class InvalidSeverityLevelError(CheckConfigurationError):
    """
    Raised when a provided severity level is not recognized by the framework.

    This error typically occurs when parsing string-based severity inputs
    (e.g. from JSON or YAML configuration files) that do not match the allowed
    levels defined in the `Severity` enum.

    Examples include:
        - normalize_severity("fatal")
        - loading a config with severity="urgent"

    This exception is used to ensure consistent error handling and reporting
    for configuration-related issues.
    """

    def __init__(self, value: str) -> None:
        """
        Initialize the exception with the invalid severity value.

        Args:
            value (str): The unrecognized severity level that triggered the error.

        The message will include the invalid value to aid debugging and error reporting.
        """
        super().__init__(f"Invalid severity level: '{value}'")
        self.value = value


class MissingCheckTypeError(CheckConfigurationError):
    """
    Raised when a configuration dictionary is missing the required 'check' field.

    This field is mandatory for the framework to identify which check type
    should be instantiated via the CheckFactory.

    This exception is typically raised during early parsing or validation
    of configuration sources (e.g. JSON, YAML).
    """

    def __init__(self) -> None:
        super().__init__(
            "Missing 'check' field in check configuration. "
            "Each check config must include a 'check' key to identify the check type."
        )


class InvalidSQLExpressionError(RuntimeCheckConfigurationError):
    """
    Raised when a SQL expression is syntactically or semantically invalid.

    This error indicates that the provided SQL expression cannot be parsed
    or executed by PySpark, typically due to syntax errors, invalid function
    calls, or other structural issues that prevent the expression from being
    evaluated.

    Examples include:
        - Malformed syntax: "age + "
        - Unbalanced parentheses: "upper(name"
        - Invalid function calls: "invalid_function(column)"
        - Dangerous operations: "DROP TABLE users"

    This exception is used to ensure consistent error handling for SQL
    expression validation in data quality checks.
    """

    def __init__(self, expression: str, error_message: str):
        """
        Initialize the exception with the invalid expression and error details.

        Args:
            expression (str): The SQL expression that failed validation.
            error_message (str): Detailed description of why the expression is invalid.

        The message will include both the expression and error details to aid
        debugging and error reporting.
        """
        super().__init__(f"Invalid SQL expression '{expression}':\n{error_message}")
        self.expression = expression
        self.error_message = error_message
