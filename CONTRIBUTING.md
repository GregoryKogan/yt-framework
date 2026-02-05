# Contributing to YT Framework

<!-- markdownlint-disable MD029 -->

Thank you for your interest in contributing to YT Framework! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How to Contribute](#how-to-contribute)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Development Workflow](#development-workflow)
- [Code Style and Conventions](#code-style-and-conventions)
- [Testing](#testing)
- [Documentation](#documentation)
- [Submitting Contributions](#submitting-contributions)
- [Additional Guidelines](#additional-guidelines)

## Code of Conduct

This project adheres to a code of conduct that all contributors are expected to follow. Please be respectful and constructive in all interactions.

## How to Contribute

Contributions come in many forms:

- **Bug Reports**: Report issues you encounter
- **Feature Requests**: Suggest new features or improvements
- **Code Contributions**: Submit pull requests with bug fixes or new features
- **Documentation**: Improve or add documentation
- **Examples**: Add example pipelines demonstrating framework features
- **Testing**: Add tests or improve test coverage

## Development Setup

### Prerequisites

- Python 3.11 or higher
- Git
- Access to YTsaurus cluster (for production mode testing)
- YT credentials (for production mode testing)

### Installation

1. **Fork and clone the repository**:

   ```bash
   git clone https://github.com/GregoryKogan/yt-framework.git
   cd yt-framework
   ```

2. **Install in editable mode**:

   ```bash
   pip install -e .
   ```

3. **Install development dependencies**:

   ```bash
   pip install -e ".[dev]"
   ```

   This installs additional tools like `pytest` and `pytest-cov` for testing.

4. **Set up YT credentials** (for production mode testing):

   Create a `secrets.env` file in any example's `configs/` directory:

   ```bash
   # configs/secrets.env
   YT_PROXY=your-yt-proxy-url
   YT_TOKEN=your-yt-token
   ```

   See [Configuration Guide](docs/configuration/index.md#secrets-management) for more details.

5. **Verify installation**:

   ```bash
   python -c "import yt_framework; print('YT Framework installed successfully')"
   ```

## Development Workflow

### Using Dev Mode

YT Framework supports a **dev mode** that simulates YT operations locally using the file system. This is perfect for development and testing without needing YT cluster access.

1. **Set mode to dev** in your pipeline config:

   ```yaml
   # configs/config.yaml
   pipeline:
     mode: "dev"
   ```

2. **Run your pipeline**:

   ```bash
   python pipeline.py
   ```

   In dev mode, tables are stored as `.jsonl` files in the `.dev/` directory, and operations run locally.

See [Dev vs Prod Guide](docs/dev-vs-prod.md) for more details.

### Creating Test Pipelines

When testing changes, create a test pipeline in the `examples/` directory:

1. Create a new example directory:

   ```bash
   mkdir -p examples/test_feature/stages/my_stage configs
   ```

2. Create `pipeline.py`:

   ```python
   from yt_framework.core.pipeline import DefaultPipeline
   
   if __name__ == "__main__":
       DefaultPipeline.main()
   ```

3. Create stage and config files following the pattern in existing examples.

4. Run the example to verify your changes work correctly.

### Running Examples

To verify your changes work with existing examples:

```bash
cd examples/01_hello_world
python pipeline.py
```

This helps ensure you haven't broken existing functionality.

### Code Organization Principles

- **Separation of Concerns**: Keep core logic, operations, and utilities separate
- **Reusability**: Write code that can be reused across different stages
- **Simplicity**: Prefer simple, readable solutions over complex ones
- **Consistency**: Follow existing patterns and conventions

## Code Style and Conventions

### Python Style

- Follow **PEP 8** style guidelines
- Use **black** or similar formatter
- Use type hints where appropriate

### Naming Conventions

- **Classes**: `PascalCase` (e.g., `BaseStage`, `DefaultPipeline`)
- **Functions and variables**: `snake_case` (e.g., `write_table`, `config_path`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_MODE`)
- **Private methods**: Prefix with underscore (e.g., `_internal_method`)

### Docstrings

Use Google-style docstrings:

```python
def write_table(self, table_path: str, rows: list) -> None:
    """Write rows to a YT table.

    Args:
        table_path: Path to the YT table (e.g., "//tmp/my_table")
        rows: List of dictionaries representing table rows

    Raises:
        ValueError: If table_path is invalid
    """
    ...
```

### Import Organization

Organize imports in this order:

1. Standard library imports
2. Third-party imports
3. Local application imports

Example:

```python
import os
from pathlib import Path
from typing import Optional

import ytsaurus_client
from omegaconf import DictConfig

from yt_framework.core.stage import BaseStage
from yt_framework.operations.table import TableOperation
```

### File Structure Conventions

- One class per file (when possible)
- Keep files focused and cohesive
- Use `__init__.py` to expose public API
- Place tests in `tests/` directory (when test suite exists)

## Testing

### Testing Approach

YT Framework uses **pytest** for testing (available as a dev dependency). While a comprehensive test suite is being developed, testing is currently done through:

1. **Dev Mode Testing**: Use dev mode to test changes locally
2. **Example Pipelines**: Run existing examples to verify compatibility
3. **Manual Testing**: Create test pipelines to exercise new features

### Running Tests

When tests are available, run them with:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=yt_framework

# Run specific test file
pytest tests/test_stage.py
```

### Testing with Dev Mode

Dev mode is ideal for testing because it:

- Simulates YT operations locally
- Doesn't require YT cluster access
- Provides fast feedback
- Creates reproducible test environments

Example:

```bash
cd examples/01_hello_world
# Ensure config.yaml has mode: "dev"
python pipeline.py
```

### Testing with Examples

Before submitting a PR, verify your changes work with multiple examples:

```bash
for example in examples/*/; do
    cd "$example"
    python pipeline.py
    cd ../..
done
```

## Documentation

### Updating Documentation

Documentation lives in the `docs/` directory:

- **Main docs**: `docs/index.md` - Installation and quick start
- **Guides**: `docs/pipelines-and-stages.md`, `docs/configuration/index.md`, etc.
- **API Reference**: `docs/reference/api.md`
- **Troubleshooting**: `docs/troubleshooting/index.md`

When adding features:

1. Update relevant documentation files
2. Add examples if applicable
3. Update API reference if adding public APIs
4. Add troubleshooting entries for common issues

### Adding Examples

Examples are valuable for demonstrating features:

1. Create a new directory in `examples/` with a descriptive name
2. Follow the structure of existing examples
3. Include a `README.md` explaining what the example demonstrates
4. Add the example to the main `README.md` examples list

### Docstring Standards

- Document all public classes and methods
- Include parameter descriptions and types
- Document return values
- Include usage examples for complex functions
- Document exceptions that may be raised

## Submitting Contributions

### Creating Issues

Before creating an issue:

1. **Search existing issues** to avoid duplicates
2. **Check documentation** to ensure it's not already covered
3. **Verify it's a bug** or clearly describe the feature request

#### Bug Reports

Include:

- Clear description of the issue
- Steps to reproduce
- Expected vs actual behavior
- Environment details (Python version, OS, etc.)
- Error messages or logs (if applicable)
- Minimal example demonstrating the issue (if possible)

#### Feature Requests

Include:

- Clear description of the feature
- Use case and motivation
- Proposed solution (if you have one)
- Alternatives considered (if any)

### Forking and Branching

1. **Fork the repository** on GitHub

2. **Create a branch** for your changes:

   ```bash
   git checkout -b feature/my-feature-name
   # or
   git checkout -b fix/bug-description
   ```

3. **Make your changes** following the guidelines above

4. **Commit your changes** (see commit message guidelines below)

5. **Push to your fork**:

   ```bash
   git push origin feature/my-feature-name
   ```

### Commit Message Guidelines

Write clear, descriptive commit messages:

- **Convention**: Follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification

Examples:

```plaintext
feat: add support for custom Docker images in Map operations
```

```plaintext
fix: fix stage discovery for nested directories
```

```plaintext
docs: update documentation with examples
```

```plaintext
test: add tests for new feature
```

```plaintext

### Pull Request Process

** Pull requests without up-to-date documentation will not be merged.**

If your changes affect user-facing functionality, APIs, configuration, or behavior, you must update the relevant documentation. This includes:

- User guides in `docs/`
- API reference in `docs/reference/api.md`
- Example code and README files
- Inline code documentation (docstrings)

1. **Update your branch** with the latest changes from main:

   ```bash
   git checkout main
   git pull upstream main
   git checkout feature/my-feature-name
   git rebase main
   ```

2. **Ensure your code follows style guidelines** <!---->

3. **Test your changes** using dev mode and examples

4. **Update documentation** if needed (see [Documentation](#documentation) section)

5. **Create a pull request** on GitHub with:
   - Clear title and description
   - Reference to related issues (if any)
   - Summary of changes
   - Testing performed

### Pull Request Checklist

Before submitting, ensure:

- [ ] Code follows style guidelines
- [ ] Changes are tested (dev mode and/or examples)
- [ ] Documentation is updated
- [ ] Commit messages are clear
- [ ] No merge conflicts with main
- [ ] Examples still work (if applicable)
- [ ] No sensitive data (credentials, tokens) in commits

## Additional Guidelines

### License

By contributing to YT Framework, you agree that your contributions will be licensed under the same license as the project.

### Maintainers

For questions or concerns, contact the maintainers:

- **Gregory Koganovsky** - <g.koganovsky@gmail.com>
- **Artem Zavarzin** - <artemutz555@gmail.com>

### Links

- **Repository**: <https://github.com/GregoryKogan/yt-framework>
- **Issues**: <https://github.com/GregoryKogan/yt-framework/issues>
- **Documentation**: See `docs/` directory

### Getting Help

- Check the [Troubleshooting Guide](docs/troubleshooting/index.md) for common issues
- Review existing [Examples](examples/) for usage patterns
- Open an issue for bugs or feature requests
- Contact maintainers for questions

Thank you for contributing to YT Framework!
