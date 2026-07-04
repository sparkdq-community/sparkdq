# Row-Level Checks

Row-level checks validate each record individually. If a row fails a check, it is marked as invalid and annotated with failure context.

| Check                                                                   | Description                                                         |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------- |
| [Column Less Than](checks/columns_comparison/column_less.md)            | Values in one column are less than another column or expression.    |
| [Column Greater Than](checks/columns_comparison/column_greater.md)      | Values in one column are greater than another column or expression. |
| [Date Between](checks/date/date_between_check.md)                       | Date values fall within a defined range.                            |
| [Date Max](checks/date/date_max_check.md)                               | Date values do not exceed a defined maximum.                        |
| [Date Min](checks/date/date_min_check.md)                               | Date values are not below a defined minimum.                        |
| [Exactly One Not Null](checks/null/exactly_one_not_null_check.md)       | Exactly one of the specified columns is non-null per row.           |
| [Is Contained In](checks/contained_in/is_contained_in_check.md)         | Column values are within a predefined set of allowed values.        |
| [Is Not Contained In](checks/contained_in/is_not_contained_in_check.md) | Column values are not within a set of forbidden values.             |
| [Not Null](checks/null/not_null_check.md)                               | Columns are expected to stay null; flags rows that contain a value. |
| [Null Check](checks/null/null_check.md)                                 | Column contains no null values.                                     |
| [Numeric Between](checks/numeric/numeric_between_check.md)              | Numeric values fall within a defined range.                         |
| [Numeric Max](checks/numeric/numeric_max_check.md)                      | Numeric values do not exceed a defined maximum.                     |
| [Numeric Min](checks/numeric/numeric_min_check.md)                      | Numeric values are not below a defined minimum.                     |
| [Timestamp Between](checks/timestamp/timestamp_between_check.md)        | Timestamp values fall within a defined range.                       |
| [Timestamp Max](checks/timestamp/timestamp_max_check.md)                | Timestamp values do not exceed a defined maximum.                   |
| [Timestamp Min](checks/timestamp/timestamp_min_check.md)                | Timestamp values are not below a defined minimum.                   |
| [Regex Match](checks/strings/regex_match_check.md)                      | String values match a given regular expression.                     |
| [String Length Between](checks/strings/string_between_length.md)        | String lengths fall within a defined range.                         |
| [String Max Length](checks/strings/string_max_length.md)                | String values do not exceed a maximum length.                       |
| [String Min Length](checks/strings/string_min_length.md)                | String values meet a minimum length requirement.                    |
