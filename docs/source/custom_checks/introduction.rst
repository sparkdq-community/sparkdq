Introduction
============

While the framework offers a growing set of built-in checks for common data quality scenarios,  
it is not intended to cover every possible validation use case out of the box.  
In real-world projects, you often need to enforce rules that are specific to your domain, business logic, or data sources.

To support these needs, the framework is built to be fully extensible.  
You can implement your own checks — whether row-level or aggregate — and register them as plugins.  
Once registered, your custom checks integrate seamlessly into the engine and can be used declaratively,  
just like the built-in ones.

This flexibility is made possible by a clean architectural principle:  
a clear **separation between check configuration and check logic**.

- The check logic is implemented in reusable Python classes that define how the validation works  
- The configuration is defined using Pydantic models that validate input and declare required parameters

Together with a central registry and factory system, this design enables a powerful plugin architecture:  
you can add new checks without modifying any part of the engine — while still benefiting from features like input validation, severity levels, and consistent result handling.

What you'll learn
-----------------

In the following sections, you'll learn how to extend the framework with your own checks — using the same foundation that powers the built-in ones.

* We’ll begin by explaining **why configuration and logic are separated**, and how that improves flexibility and maintainability.

* Next, you’ll learn how the framework applies the **Factory Pattern** and a **central registry** to dynamically resolve and instantiate checks based on configuration alone.

* Finally, we’ll walk through two practical examples: one for a **row-level check**, and one for an **aggregate check**. You’ll see how to implement validation logic, define and register configuration classes, and use your custom checks declaratively — in Python, JSON, or YAML.


.. raw:: html

   <hr>

🚀 **Next Step**: By the end, you’ll be able to build custom, production-ready checks that feel like a natural part of the framework.
