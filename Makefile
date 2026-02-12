.PHONY: help format lint check test clean install run

# Default target
help:
	@echo "Available targets:"
	@echo "  make format     - Format code with black and isort"
	@echo "  make lint       - Run all linting checks (format check + import check)"
	@echo "  make check      - Check formatting and imports without modifying files"
	@echo "  make test       - Run pytest tests"
	@echo "  make clean      - Clean Python cache files"
	@echo "  make install    - Install dependencies with uv"
	@echo "  make run        - Run the data pipeline (main.py)"

# Source directories
SRC_DIR := src
TEST_DIR := tests
PYTHON_FILES := $(shell find $(SRC_DIR) $(TEST_DIR) -name '*.py' -not -path '*/\.*' -not -path '*/__pycache__/*' -not -path '*/_delta_log/*')

# Format code with black and isort
format:
	@echo "Formatting code with black..."
	uv run black $(SRC_DIR) $(TEST_DIR)
	@echo "Sorting imports with isort..."
	uv run isort $(SRC_DIR) $(TEST_DIR)
	@echo "Formatting complete!"

# Check formatting without modifying files
check-format:
	@echo "Checking code formatting with black..."
	uv run black --check $(SRC_DIR) $(TEST_DIR) || (echo "Code formatting issues found. Run 'make format' to fix." && exit 1)
	@echo "Checking import sorting with isort..."
	uv run isort --check-only $(SRC_DIR) $(TEST_DIR) || (echo "Import sorting issues found. Run 'make format' to fix." && exit 1)
	@echo "All formatting checks passed!"

# Run all linting checks
lint: check-format
	@echo "Linting complete!"

# Alias for lint (check without modifying)
check: lint

# Run tests
test:
	@echo "Running tests with pytest..."
	PYTHONPATH=$(SRC_DIR) uv run pytest $(TEST_DIR) -v

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	PYTHONPATH=$(SRC_DIR) uv run pytest $(TEST_DIR) --cov=$(SRC_DIR) --cov-report=html --cov-report=term

# Clean Python cache files
clean:
	@echo "Cleaning Python cache files..."
	find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -r {} + 2>/dev/null || true
	@echo "Clean complete!"

# Install dependencies
install:
	@echo "Installing dependencies with uv..."
	uv sync
	@echo "Installation complete!"

# Run the data pipeline
run:
	@echo "Running pipeline..."
	PYTHONPATH=$(SRC_DIR) uv run python main.py

# Run format, lint, and test
all: format lint test
	@echo "All checks passed!"
