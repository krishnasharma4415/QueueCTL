.PHONY: help install install-dev test lint format type-check clean demo

help:
	@echo "Available commands:"
	@echo "  install      Install package"
	@echo "  install-dev  Install package with development dependencies"
	@echo "  test         Run tests"
	@echo "  lint         Run linting"
	@echo "  format       Format code"
	@echo "  type-check   Run type checking"
	@echo "  clean        Clean up generated files"
	@echo "  demo         Run demo script"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest

test-cov:
	pytest --cov=queuectl --cov-report=html --cov-report=term

lint:
	ruff check queuectl/ tests/

format:
	black queuectl/ tests/
	ruff check --fix queuectl/ tests/

type-check:
	mypy queuectl/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

demo:
	@echo "Running QueueCTL demo..."
	bash scripts/demo.sh