# Contributing to SparkDQ

Thank you for your interest in contributing to SparkDQ. This document outlines the process for reporting issues, proposing changes, and submitting pull requests.

## Project Scope

SparkDQ provides declarative data quality checks for PySpark workloads. Contributions should align with the project's focus on:

- Simple, expressive, and testable check definitions
- Row-level and aggregate-level validations
- YAML/Pydantic-based configuration with strong test coverage

If you are unsure whether a contribution fits the project's direction, open an issue to discuss it before investing significant effort.

## Reporting Issues

Use [GitHub Issues](https://github.com/sparkdq-community/sparkdq/issues) to report bugs, request features, or suggest improvements. Before opening a new issue, search existing issues to avoid duplicates.

When reporting a bug, include:

- A minimal reproducible example
- The Python and PySpark versions in use
- The full error message or unexpected behavior

## Development Workflow

### 1. Fork and clone the repository

```shell
git clone https://github.com/sparkdq-community/sparkdq.git
cd sparkdq
```

### 2. Create a feature branch

```shell
git checkout -b feature/your-feature-name
```

### 3. Install dependencies

Dependencies are managed via `uv`:

```shell
uv sync
```

### 4. Implement your changes

Include unit tests for all new functionality. Update the documentation if the change affects public APIs or user-facing behavior.

### 5. Run the test suite

```shell
pytest --cov=sparkdq --cov-report=term-missing
```

All tests must pass before submitting a pull request.

### 6. Commit your changes

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```shell
git commit -m "feat: add null-ratio aggregate check"
git commit -m "fix: handle empty DataFrame in row-level engine"
git commit -m "docs: update custom check implementation guide"
```

### 7. Open a pull request

Push your branch and open a pull request against `main`. In the pull request description, include:

- A summary of the change and its motivation
- References to any related issues
- Notes on testing approach if non-obvious

## Code Style

- Code formatting is enforced via `ruff`
- Type annotations are required for all public interfaces
- Docstrings follow the Google style convention
- Pull requests should be focused and minimal in scope

## Good First Issues

Issues labeled [`good first issue`](https://github.com/sparkdq-community/sparkdq/issues?q=is%3Aopen+label%3A%22good+first+issue%22) are suitable entry points for new contributors.
