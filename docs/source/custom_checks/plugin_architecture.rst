Plugin Architecture
===================

What is the Factory Pattern?
----------------------------

The Factory Pattern is a common software design principle that helps decouple object creation from the
rest of your code.   Instead of instantiating classes manually using something like ``MyCheck(...)``, you use
a factory — a reusable component that knows how to create objects based on input data or configuration.

This is particularly useful when:

- The exact class to instantiate depends on a string or external input (like JSON or YAML)
- You want to validate and control object creation in one central place
- Your system is designed to be extensible and plugin-based

**In simple terms**:
the factory takes a description of what you want, and returns a ready-to-use object — without you
needing to know the implementation details.

How is the Factory Pattern used in this framework?
--------------------------------------------------

This framework uses the Factory Pattern to dynamically create checks from **declarative configurations**,
typically provided as a list of dictionaries — for example from a YAML file. At the center of this
mechanism is the ``CheckFactory``, which turns each configuration dictionary into a fully initialized check instance:

Workflow
^^^^^^^^

1. It receives a list of configuration entries like:

   .. code-block:: python

      config = [
          {"check": "null-check", "check-id": "c1", "column": "age"},
          {"check": "positive-value", "check-id": "c2", "column": "salary"}
      ]

2. For each entry, it uses the ``check`` to look up the matching `CheckConfig` class via the central **registry**.
3. It creates and validates the config instance using `pydantic`.
4. It calls ``.to_check()`` on the config to instantiate the actual `Check` object.

All in the Background
^^^^^^^^^^^^^^^^^^^^^

These steps are executed **automatically in the background** when you call:

.. code-block:: python

   from sparkdq.management import CheckSet

   check_set = CheckSet().add_checks_from_dicts(config)

The registry is an essential part of this mechanism: it allows the factory to resolve config classes
dynamically based on the `check` name. Once a config is validated, the factory delegates to ``.to_check()``
to create the corresponding check object.


The Role of the Registry and the Plugin Architecture
----------------------------------------------------

The registry is a central component that maps `check_name` (e.g. `null-check`) to their `CheckConfig` classes.  
This mapping is established automatically when a configuration class is decorated with:

.. code-block:: python

   @register_check_config(check_name="null-check")

This decorator registers the config class under the given name and makes it discoverable to the factory.

Thanks to this mechanism, the ``CheckFactory`` can create checks without knowing anything about the actual
classes. As long as the config class is properly registered, the factory can find it, instantiate it,
validate it, and produce the corresponding check instance — all based on the declared `check_name`.

This dynamic lookup mechanism enables the framework’s **plugin architecture**:  
You can define and register new checks anywhere in your codebase — even in external packages — and they will
be picked up automatically, without modifying the engine or factory logic. The only requirement is that each
config class is registered using ``@register_check_config(...)`` and declares its corresponding `check_class`.

To ensure that all available checks are registered before use, the module containing the config classes must be imported.  
This is done explicitly using the utility function ``load_config_module(...)``:

.. code-block:: python

   from sparkdq.plugin import load_config_module

   load_config_module("your_custom_checks")

This triggers the registration logic by importing the specified module using ``importlib``.  
It is a clean and safe way to make the plugin system work reliably.

.. raw:: html

   <hr>

🚀 **Next Step**: In the next section, you’ll learn how to implement custom row-level and aggregate
checks — and make them fully declarative, extensible, and production-ready.
