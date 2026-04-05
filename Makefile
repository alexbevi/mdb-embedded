PYTHON  ?= python3
CARGO   ?= cargo
RUST    := rust/Cargo.toml

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

install: ## Install smongo (builds Rust extension via maturin)
	pip install maturin
	maturin develop --release

install-dev: install ## Install with dev + all optional deps
	$(PYTHON) -m pip install -e ".[dev,all]"

setup: install-dev ## Full dev setup: deps + pre-commit hooks
	pre-commit install

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
.PHONY: build build-rust build-debug

build: build-rust install ## Full build: Rust + Python package

build-rust: ## Build and lint the Rust crate
	$(CARGO) test --manifest-path $(RUST)
	$(CARGO) clippy --manifest-path $(RUST) -- -D warnings

build-debug: ## Build Rust extension in debug mode (faster compile)
	maturin develop

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
.PHONY: lint format typecheck check

lint: ## Run ruff linter
	$(PYTHON) -m ruff check $(SRC)

format: ## Run ruff formatter
	$(PYTHON) -m ruff format $(SRC) $(TESTS)

typecheck: ## Run mypy strict type checking
	$(PYTHON) -m mypy smongo/ web_app.py

check: lint typecheck build-rust ## Run all static checks (lint + types + Rust)

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------
.PHONY: test test-rust test-unit test-integration test-perf test-all

test: ## Run unit test suite (no Docker, no network)
	pytest tests -q

test-rust: ## Run Rust-only tests (cargo test + clippy)
	$(CARGO) test --manifest-path $(RUST)
	$(CARGO) clippy --manifest-path $(RUST) -- -D warnings

test-unit: test ## Alias for `make test`

test-integration: ## Run integration tests (requires Docker MongoDB)
	pytest tests/integration -m integration -v --override-ini="addopts="

test-perf: ## Run performance benchmarks
	pytest tests/performance -m performance --benchmark-only -q --override-ini="addopts="

test-all: test-rust test test-integration ## Run everything: Rust + unit + integration

# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------
.PHONY: coverage coverage-html

coverage: ## Run tests with coverage report (70% enforced)
	pytest tests -m "not performance" \
		--cov=smongo --cov=web_app \
		--cov-report=term-missing

coverage-html: ## Generate HTML coverage report
	pytest tests -m "not performance" \
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
	$(PYTHON) demo.py

web: ## Start the web dashboard (localhost:5000)
	$(PYTHON) web_app.py

wire: ## Start the wire protocol server (localhost:27017)
	$(PYTHON) -m smongo.wire --port 27017

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
.PHONY: clean clean-rust clean-all

clean: ## Remove Python build artifacts and caches
	rm -rf build/ dist/ *.egg-info .eggs
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	rm -rf htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true

clean-rust: ## Remove Rust build artifacts
	$(CARGO) clean --manifest-path $(RUST)

clean-all: clean clean-rust ## Remove all build artifacts (Python + Rust)
