# 🤝 Contributing to SparkDQ

🎉 First off, thank you for considering contributing to SparkDQ!Your help is greatly appreciated and helps make this project better for everyone. 🚀

This document explains how to contribute effectively. Whether it's fixing a bug, improving the documentation, or proposing a new check — all contributions are welcome.

## 🎯 Project Scope

SparkDQ provides declarative data quality checks for PySpark workloads.

The focus is on:

* ✅ Simple, expressive, and testable checks

* 🧪 Both row-level and aggregate-level validations

* 🛠️ Easy YAML/Pydantic configuration and strong test coverage

If you’re unsure whether a contribution fits, feel free to open an issue and start the conversation! 💬

## 🛠️ How to Contribute

### 🐛 Issues

Please use GitHub Issues to:

* Report bugs 🐞

* Request features 💡

* Suggest improvements ✍️

If you find an issue you'd like to work on, comment on it to avoid duplication.

New ideas? Use the enhancement or discussion label.

### 👩‍💻 Code Contributions

#### 1️⃣ Fork the repository and clone it locally:

```shell
git clone https://github.com/sparkdq-community/sparkdq.git
cd sparkdq
```

#### 2️⃣ Create a new branch:

```shell
git checkout -b feature/your-feature-name
```

#### 3️⃣ Install dependencies (recommended via uv):

```shell
uv sync
```

#### 4️⃣ Add your changes (including tests if applicable).

#### 5️⃣ Run the test suite:

```shell
pytest --cov=sparkdq --cov-report=term-missing
```

#### 6️⃣ Commit your changes following Conventional Commits:

```shell
git commit -m "feat: add new check for XYZ"
```

#### 7️⃣ Push your branch and open a pull request 🚀:

Please describe the problem, solution, and include examples if helpful.

* 🧪 Code Style and Guidelines

* 🖤 Use consistent code style (black, ruff, or flake8 recommended).

* ✅ Write unit tests for all new functionality.

* 📖 Update the documentation if needed (Sphinx docstrings, YAML examples).

* 💎 Keep pull requests focused and as small as reasonably possible.

## 🌱 Good First Issues

We label beginner-friendly tasks with good first issue 🟢.Check the open issues to get started!

## 🤗 Community and Behavior

Please be respectful and kind in all interactions.We follow the Code of Conduct — thank you for helping keep this a welcoming place! ❤️

## 🙏 Thank You

Your contribution helps improve SparkDQ — and you make a real impact!Feel free to reach out via GitHub Issues or Discussions if you need help getting started.

Happy contributing! 🧡🚀
