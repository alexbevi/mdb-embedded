CARGO   ?= cargo
RUST    := rust/Cargo.toml
# Repository root (Makefile lives here). All maturin/pip installs must run from here
# so pyproject.toml [tool.maturin] (manifest-path, module-name) stays consistent.
ROOT    := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
# Same interpreter for pip and maturin: prefer ./.venv when present (avoids pyenv vs .venv split).
ifeq ($(origin PYTHON),undefined)
PYTHON := $(shell test -x "$(ROOT)/.venv/bin/python" && echo "$(ROOT)/.venv/bin/python" || command -v python3)
endif

SRC     := smongo web_app.py demo.py
TESTS   := tests

# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
.PHONY: install setup install-dev

install: ## Editable install: smongo + PyO3 extension (run from repo root)
	cd $(ROOT) && $(PYTHON) -m pip install maturin
	cd $(ROOT) && $(PYTHON) -m pip install -e .

install-dev: ## Editable install with dev + optional extras (flask, vector, …)
	cd $(ROOT) && $(PYTHON) -m pip install maturin
	cd $(ROOT) && $(PYTHON) -m pip install -e ".[dev,all]"

setup: install-dev ## Full dev setup: deps + pre-commit hooks
	cd $(ROOT) && pre-commit install

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
.PHONY: build build-rust build-debug

build: build-rust install ## Full build: Rust workspace tests + editable Python package

build-rust: ## Build and lint the Rust workspace
	cd $(ROOT) && $(CARGO) test --manifest-path $(RUST)
	cd $(ROOT) && $(CARGO) clippy --manifest-path $(RUST) -- -D warnings

build-debug: ## Rebuild only the PyO3 extension (debug, fast iteration)
	cd $(ROOT) && $(PYTHON) -m maturin develop --manifest-path rust/smongo-py/Cargo.toml

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
.PHONY: lint format typecheck check

lint: ## Run ruff linter
	cd $(ROOT) && $(PYTHON) -m ruff check $(SRC)

format: ## Run ruff formatter
	cd $(ROOT) && $(PYTHON) -m ruff format $(SRC) $(TESTS)

typecheck: ## Run mypy strict type checking
	cd $(ROOT) && $(PYTHON) -m mypy smongo/ web_app.py

check: lint typecheck build-rust ## Run all static checks (lint + types + Rust)

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------
.PHONY: test test-rust test-unit test-integration test-perf test-all

test: ## Run unit test suite (no Docker, no network)
	cd $(ROOT) && $(PYTHON) -m pytest tests -q

test-rust: ## Run Rust-only tests (cargo test + clippy)
	cd $(ROOT) && $(CARGO) test --manifest-path $(RUST)
	cd $(ROOT) && $(CARGO) clippy --manifest-path $(RUST) -- -D warnings

test-unit: test ## Alias for `make test`

test-integration: ## Run integration tests (requires Docker MongoDB)
	cd $(ROOT) && $(PYTHON) -m pytest tests/integration -m integration -v --override-ini="addopts="

test-perf: ## Run performance benchmarks
	cd $(ROOT) && $(PYTHON) -m pytest tests/performance -m performance --benchmark-only -q --override-ini="addopts="

test-all: test-rust test test-integration ## Run everything: Rust + unit + integration

# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------
.PHONY: coverage coverage-html

coverage: ## Run tests with coverage report (70% enforced)
	cd $(ROOT) && $(PYTHON) -m pytest tests -m "not performance" \
		--cov=smongo --cov=web_app \
		--cov-report=term-missing

coverage-html: ## Generate HTML coverage report
	cd $(ROOT) && $(PYTHON) -m pytest tests -m "not performance" \
		--cov=smongo --cov=web_app \
		--cov-report=html --cov-report=term-missing
	@echo "  open htmlcov/index.html"

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
.PHONY: docker docker-up docker-down docker-build

docker-up: ## Start full stack (app + MongoDB) via docker compose
	docker compose up --build -d
	@echo "  Dashboard:  http://localhost:5000"
	@echo "  Compass:    mongodb://localhost:27018"

docker-down: ## Stop and remove containers
	docker compose down

docker-build: ## Build Docker image only
	docker compose build

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
.PHONY: demo web wire

demo: ## Run the standalone CLI demo
	cd $(ROOT) && $(PYTHON) demo.py

web: ## Start the web dashboard (localhost:5000)
	cd $(ROOT) && $(PYTHON) web_app.py

wire: ## Start the wire protocol server (localhost:27017)
	cd $(ROOT) && $(PYTHON) -m smongo.wire --port 27017

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
.PHONY: clean clean-rust clean-all

clean: ## Remove Python build artifacts and caches
	cd $(ROOT) && rm -rf build/ dist/ *.egg-info .eggs
	cd $(ROOT) && rm -rf .pytest_cache .mypy_cache .ruff_cache
	cd $(ROOT) && rm -rf htmlcov .coverage
	cd $(ROOT) && find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	cd $(ROOT) && find . -type f -name '*.pyc' -delete 2>/dev/null || true

clean-rust: ## Remove Rust build artifacts
	cd $(ROOT) && $(CARGO) clean --manifest-path $(RUST)

clean-all: clean clean-rust ## Remove all build artifacts (Python + Rust)
