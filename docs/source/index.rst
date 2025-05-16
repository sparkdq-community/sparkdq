.. sparkdq documentation master file, created by
   sphinx-quickstart on Fri Apr  4 15:04:58 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SparkDQ — Data Quality Validation
=================================

.. include:: ../../README.md
   :parser: myst_parser.sphinx_
   :start-after: Apache Spark
   :end-before: <!-- doc-link-start -->

.. include:: ../../README.md
   :parser: myst_parser.sphinx_
   :start-after: <!-- doc-link-end -->

.. toctree::
   :maxdepth: 2
   :caption: Getting Started
   :hidden:

   getting_started/introduction
   getting_started/defining_checks
   getting_started/validation_dataframes
   getting_started/applying_validation

.. toctree::
   :maxdepth: 2
   :caption: Built-In Checks
   :hidden:

   built_in_checks/row_level
   built_in_checks/aggregate

.. toctree::
   :maxdepth: 2
   :caption: Custom Checks
   :hidden:

   custom_checks/introduction
   custom_checks/separation_logic
   custom_checks/plugin_architecture
   custom_checks/implementation/structure

.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   api_docs
   examples
