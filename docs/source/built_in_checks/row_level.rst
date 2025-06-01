Row-Level Checks
================

.. toctree::
   :maxdepth: 1
   :caption: Built-in Checks
   :hidden:

   checks/columns_comparison/column_less
   checks/columns_comparison/column_greater
   checks/date/date_between_check
   checks/date/date_max_check
   checks/date/date_min_check
   checks/null/exactly_one_not_null_check
   checks/contained_in/is_contained_in_check
   checks/contained_in/is_not_contained_in_check
   checks/null/not_null_check
   checks/null/null_check
   checks/numeric/numeric_between_check
   checks/numeric/numeric_max_check
   checks/numeric/numeric_min_check
   checks/strings/regex_match_check
   checks/strings/string_between_length
   checks/strings/string_max_length
   checks/strings/string_min_length
   checks/timestamp/timestamp_between_check
   checks/timestamp/timestamp_max_check
   checks/timestamp/timestamp_min_check

.. csv-table::
    :header: "Check", "Description"
    :widths: 20, 80

    ":ref:`column-less-than-check` ", "Ensures that values in one column are less than another column or expression result."
    ":ref:`column-greater-than-check` ", "Ensures that values in one column are greater than another column or expression result."
    ":ref:`date-between-check` ", "Ensures that date column values are within a defined range."
    ":ref:`date-max-check` ", "Ensures that date column values are less than a defined maximum date."
    ":ref:`date-min-check` ", "Ensures that date column values are greater than a defined minimum date."
    ":ref:`exactly_one_not_null_check` ", "Validates that exactly one of the specified columns is non-null per row."
    ":ref:`is-contained-in-check` ", "Ensures that column values are contained within a predefined set of allowed values."
    ":ref:`is-not-contained-in-check` ", "Ensures that column values are not contained within a set of forbidden values."
    ":ref:`not_null_check` ", "Checks whether the specified column contains at least one non-null value."
    ":ref:`null_check` ", "Verifies whether a given column contains any null values."
    ":ref:`numeric-between-check` ", "Ensures that numeric column values are within a defined inclusive range."
    ":ref:`numeric-max-check` ", "Ensures that numeric column values are less than a defined maximum."
    ":ref:`numeric-min-check` ", "Ensures that numeric column values are greater than a defined minimum."
    ":ref:`timestamp-between-check` ", "Ensures that timestamp column values are within a defined inclusive range between a minimum and maximum timestamp."
    ":ref:`timestamp-max-check` ", "Ensures that timestamp column values are less than a defined maximum timestamp."
    ":ref:`timestamp-min-check` ", "Ensures that timestamp column values are greater than a defined timestamp."
    ":ref:`regex_match_check`", "Validates that string values match a given regular expression pattern."
    ":ref:`string_length_between_check`", "Checks that string lengths fall within a specified range."
    ":ref:`string_max_length_check`", "Ensures that string values do not exceed a maximum length."
    ":ref:`string_min_length_check`", "Validates that string values in a column meet a minimum length requirement."
