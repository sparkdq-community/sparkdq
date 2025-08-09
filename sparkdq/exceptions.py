class RuntimeCheckConfigurationError(Exception):
    """
    Base exception for configuration errors detected during check execution.

    Represents configuration issues that can only be identified when checks
    are applied to actual datasets, such as column references that don't exist
    in the target data or invalid data type assumptions. These errors indicate
    mismatches between check configuration and runtime data characteristics.
    """


class CheckConfigurationError(Exception):
    """
    Base exception for static configuration validation errors.

    Represents configuration issues that can be detected during check definition
    or setup, independent of any specific dataset. These errors indicate
    fundamental problems with check parameters, logical inconsistencies, or
    missing required configuration elements.
    """


class MissingColumnError(RuntimeCheckConfigurationError):
    """
    Exception raised when a check references a non-existent column.

    Indicates a mismatch between the check configuration and the actual dataset
    schema, where the check expects a column that is not present in the target
    DataFrame. This typically suggests either incorrect check configuration or
    unexpected changes in the data schema.
    """

    def __init__(self, column: str, available: list[str]):
        super().__init__(f"Column '{column}' not found. Available columns: {available}")


class MissingCheckSetError(RuntimeCheckConfigurationError):
    """
    Exception raised when attempting to execute validation without configured checks.

    Indicates that the validation engine was invoked without a properly assigned
    CheckSet, representing a programming error in the validation setup sequence.
    This error prevents execution attempts on improperly initialized engines.
    """

    def __init__(self) -> None:
        super().__init__(
            "No CheckSet has been assigned to the engine. "
            "Use `engine.set_check_set(check_set)` before running validation."
        )


class InvalidCheckConfigurationError(CheckConfigurationError):
    """
    Exception raised for logically inconsistent check configurations.

    Represents configuration errors where individual parameters are valid but
    their combination creates logical contradictions or impossible conditions.
    These errors are typically detected during static configuration validation
    before any data processing begins.
    """


class MissingReferenceDatasetError(RuntimeCheckConfigurationError):
    """
    Exception raised when a check requests an unavailable reference dataset.

    Indicates that a check requiring external reference data cannot locate
    the specified dataset in the current validation context, suggesting
    either missing reference data injection or incorrect dataset naming.
    """

    def __init__(self, name: str) -> None:
        super().__init__(f"Reference dataset '{name}' not found.")
        self.name = name


class InvalidSeverityLevelError(CheckConfigurationError):
    """
    Exception raised for unrecognized severity level specifications.

    Indicates that a severity value provided through configuration sources
    does not match any of the defined severity levels in the framework.
    This error ensures that only valid severity classifications are accepted,
    maintaining consistency in failure handling behavior.
    """

    def __init__(self, value: str) -> None:
        """
        Initialize the exception with the invalid severity specification.

        Args:
            value (str): The unrecognized severity level that caused the error.
        """
        super().__init__(f"Invalid severity level: '{value}'")
        self.value = value


class MissingCheckTypeError(CheckConfigurationError):
    """
    Exception raised when check configuration lacks the required type identifier.

    Indicates that a configuration dictionary is missing the mandatory 'check'
    field that specifies which check implementation should be instantiated.
    This error prevents the CheckFactory from resolving the appropriate
    check type during configuration processing.
    """

    def __init__(self) -> None:
        super().__init__(
            "Missing 'check' field in check configuration. "
            "Each check config must include a 'check' key to identify the check type."
        )


class InvalidSQLExpressionError(RuntimeCheckConfigurationError):
    """
    Exception raised for invalid or unsafe SQL expressions in check configurations.

    Indicates that a SQL expression provided to a check cannot be parsed or
    executed safely by PySpark, either due to syntax errors, semantic issues,
    or security concerns. This validation prevents potentially dangerous or
    malformed expressions from being executed against datasets.
    """

    def __init__(self, expression: str, error_message: str):
        """
        Initialize the exception with the problematic expression and diagnostic information.

        Args:
            expression (str): The SQL expression that failed validation.
            error_message (str): Detailed explanation of the validation failure.
        """
        super().__init__(f"Invalid SQL expression '{expression}':\n{error_message}")
        self.expression = expression
        self.error_message = error_message
