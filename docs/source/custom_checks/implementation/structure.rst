Implementation
==============

In the previous sections, we explained how the framework separates configuration and logic into two distinct layers:  
a ``CheckConfig`` class to declare and validate parameters, and a ``Check`` class to implement the actual validation logic.

You also learned how the registry and factory work together to dynamically resolve and instantiate checks based on configuration.
This plugin-based design does not only apply to built-in checks — it is intentionally built to be extensible.  
You can define your own custom checks in the same way, and use them **declaratively** via YAML, JSON, or Python dictionaries — exactly like the built-in ones.

In this section, you will learn:

- How to write your own row-level or aggregate check
- How to connect it to a ``CheckConfig`` class
- How to register it using ``@register_check_config`` and ``load_config_module``
- How to use it via declarative configuration and the ``CheckFactory``

Once registered, your custom check becomes a first-class citizen in the framework — fully compatible with the factory, the engine, and all standard tooling.

Recommended Folder Structure for Custom Checks
----------------------------------------------

To keep your custom checks clean and maintainable, we recommend placing them in a dedicated Python package — for example:

.. code-block:: text

   myproject/
   └── custom_checks/
       ├── __init__.py
       ├── custom_row_check.py
       └── custom_aggregate_check.py

Each module (e.g. `custom_row_check.py`, `custom_aggregate_check.py`) should contain one or more
custom `Check` implementations and their corresponding `CheckConfig` classes.

To make all config classes available for registration, the `__init__.py` file should **explicitly expose** them:

.. code-block:: python

   from .custom_row_check import MyRowCheckConfig
   from .custom_aggregate_check import MyAggregateCheckConfig

   __all__ = ["MyAggregateCheckConfig", "MyRowCheckConfig"]

This ensures that importing the `custom_checks` package will automatically trigger the decorators like:

.. code-block:: python

   @register_check_config(check_name="my-custom-row-check")
   class MyRowCheckConfig(BaseRowCheckConfig):
       ...

With this structure in place, you can load and register all your custom checks with a single line:

.. code-block:: python

   from sparkdq.plugin import load_config_module

   load_config_module("myproject.custom_checks")

This approach has several benefits:

- ✅ You only import **one module**, but all configs get registered
- ✅ Keeps custom logic modular and testable
- ✅ Ensures compatibility with declarative config loading (JSON, YAML, etc.)

By following this convention, your custom checks behave exactly like the built-in.

.. toctree::
   :maxdepth: 1
   :caption: Built-in Checks
   :hidden:

   row_level
   aggregate

.. raw:: html

   <hr>

🚀 **Next Step**: Next, we’ll walk through the implementation of a custom row-level check — step by step.