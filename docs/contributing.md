# Contributing to geoparquet-io

Thank you for your interest in contributing to geoparquet-io! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.9 or higher
- [uv](https://docs.astral.sh/uv/) package manager (recommended) or pip

### Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/cholmes/geoparquet-io.git
   cd geoparquet-io
   ```

2. **Install uv** (if not already installed)
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies**
   ```bash
   uv sync --all-extras
   ```

4. **Install pre-commit hooks**
   ```bash
   uv run pre-commit install
   ```

   This ensures code is automatically formatted and linted before each commit.

## Development Workflow

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=geoparquet_io --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_sort.py

# Run specific test
uv run pytest tests/test_sort.py::test_hilbert_order

# Skip slow/network tests
uv run pytest -m "not slow and not network"
```

### Code Style

We use [ruff](https://github.com/astral-sh/ruff) for linting and formatting.

```bash
# Format code
uv run ruff format .

# Check formatting
uv run ruff format --check .

# Run linter
uv run ruff check .

# Fix auto-fixable issues
uv run ruff check --fix .
```

**Style Guidelines:**
- Line length: 100 characters
- Follow PEP 8 conventions
- Use double quotes for strings
- Add docstrings to all public functions and classes
- Use type hints where helpful

### Pre-commit Hooks

Pre-commit hooks run automatically before each commit. They will:
- Format code with ruff
- Run linting checks
- Remove trailing whitespace
- Fix end-of-file issues
- Check YAML and TOML syntax

To run hooks manually:
```bash
uv run pre-commit run --all-files
```

## Making Changes

### Branch Naming

- Feature: `feature/description` (e.g., `feature/add-streaming-support`)
- Bug fix: `fix/description` (e.g., `fix/bbox-metadata-issue`)
- Documentation: `docs/description` (e.g., `docs/update-readme`)

### Commit Messages

Follow conventional commit format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(cli): add streaming mode for large files
fix(bbox): correct metadata format for GeoParquet 1.1
docs(readme): update installation instructions
test(partition): add edge case tests for empty files
```

### Pull Request Process

1. **Create a new branch** from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Write code
   - Add/update tests
   - Update documentation

3. **Ensure tests pass**
   ```bash
   uv run pytest
   ```

4. **Ensure code is formatted**
   ```bash
   uv run ruff format .
   uv run ruff check .
   ```

5. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat(scope): description"
   ```

6. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request**
   - Go to GitHub and create a PR from your branch to `main`
   - Fill in the PR template with a clear description
   - Link any related issues
   - Request review from maintainers

### Pull Request Requirements

Before submitting a PR, ensure:

- [ ] All tests pass (`uv run pytest`)
- [ ] Code coverage is maintained or improved
- [ ] Code is formatted (`uv run ruff format --check .`)
- [ ] Linting passes (`uv run ruff check .`)
- [ ] Documentation is updated (README, docstrings, etc.)
- [ ] CHANGELOG.md is updated (for user-facing changes)
- [ ] Commit messages follow conventional commit format

## Testing Guidelines

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_*.py`
- Name test functions `test_*`
- Use descriptive test names that explain what is being tested
- Group related tests in classes (e.g., `TestHilbertSort`)

### Test Structure

```python
def test_feature_description():
    """Brief description of what this test verifies."""
    # Arrange
    input_data = create_test_data()

    # Act
    result = function_under_test(input_data)

    # Assert
    assert result == expected_value
```

### Test Markers

Use pytest markers for special test categories:

```python
import pytest

@pytest.mark.slow
def test_large_file_processing():
    """Test that takes a long time to run."""
    pass

@pytest.mark.network
def test_remote_file_access():
    """Test that requires network access."""
    pass
```

### Test Fixtures

Add reusable fixtures to `tests/conftest.py`:

```python
import pytest

@pytest.fixture
def sample_geoparquet():
    """Provides a sample GeoParquet file for testing."""
    return "tests/data/sample.parquet"
```

## Code Review

### For Contributors

- Respond to feedback promptly
- Be open to suggestions and constructive criticism
- Keep discussions focused and professional
- Update your PR based on feedback

### For Reviewers

- Be respectful and constructive
- Explain the reasoning behind suggestions
- Approve when the code meets quality standards
- Help contributors improve their submissions

## Release Process

(For maintainers only)

1. Update version in `pyproject.toml` and `geoparquet_io/cli/main.py`
2. Update `CHANGELOG.md` with release notes
3. Create and push a git tag: `git tag v0.x.0 && git push origin v0.x.0`
4. GitHub Actions will automatically build and publish to PyPI
5. Create a GitHub release with the changelog content

## Questions?

- Open an issue for bug reports or feature requests
- Use GitHub Discussions for questions
- Check existing issues and PRs before creating new ones

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help create a welcoming environment
- Report unacceptable behavior to maintainers

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
