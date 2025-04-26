from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict


@dataclass(frozen=True)
class ValidationSummary:
    """
    Represents a summary of validation results including counts of records and pass/fail statistics.

    Attributes:
        total_records (int): Total number of records processed.
        passed_records (int): Number of records that passed validation.
        failed_records (int): Number of records that failed validation.
        warning_records (int): Number of records with warning-level checks (but still passed).
        pass_rate (float): Ratio of passed records to total records.
        timestamp (datetime): Timestamp of when the summary was created.
    """

    total_records: int
    passed_records: int
    failed_records: int
    warning_records: int
    pass_rate: float
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
