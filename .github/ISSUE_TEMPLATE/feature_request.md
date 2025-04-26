name: 💡 Feature Request
description: Suggest an idea or enhancement for SparkDQ.
labels: [enhancement]
title: "[FEATURE] <short description>"
body:
  - type: markdown
    attributes:
      value: |
        Thank you for taking the time to suggest a feature! 💡  
        Please help us understand your idea better by filling out the following details.

  - type: input
    id: summary
    attributes:
      label: 📝 Feature Summary
      description: Please provide a short description of the feature you'd like to see.
      placeholder: "Example: Add support for regex-based column name matching."
    validations:
      required: true

  - type: textarea
    id: motivation
    attributes:
      label: 🎯 Motivation
      description: |
        Why do you need this feature?  
        What problem does it solve?  
        How would it improve your workflow?
      placeholder: |
        "In my use case, I often have columns with variable names (e.g. sensor_01, sensor_02, ...).
        I'd like to be able to apply checks on these dynamically."
    validations:
      required: true

  - type: textarea
    id: proposed_solution
    attributes:
      label: 💡 Proposed Solution / Idea
      description: |
        If you have an idea of how this feature could work or be implemented, feel free to share it here.
        Sketch out an example YAML config or usage if possible.
      placeholder: |
        Example:
        - check: regex-null-check
          check_id: check_sensor_columns
          pattern: "^sensor_\\d+$"
          severity: warning

  - type: textarea
    id: alternatives
    attributes:
      label: 🔄 Alternatives Considered
      description: Did you consider any other solutions or workarounds?

  - type: textarea
    id: additional_context
    attributes:
      label: 💬 Additional Context
      description: Add any other context, screenshots, or related issues here.
