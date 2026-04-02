PYTHON ?= python3

SRC := smongo web_app.py demo.py
TESTS := tests

.PHONY: install-test lint format test integration perf coverage typecheck

install-test:
	$(PYTHON) -m pip install -e ".[dev,all]"

lint:
	$(PYTHON) -m ruff check $(SRC)

format:
	$(PYTHON) -m ruff format $(SRC) $(TESTS)

test:
	pytest tests -q

integration:
	pytest tests/integration -m integration -v --override-ini="addopts="

perf:
	pytest tests/performance -m performance --benchmark-only -q --override-ini="addopts="

coverage:
	pytest tests -m "not performance" --cov=smongo --cov=web_app --cov-report=term-missing

typecheck:
	$(PYTHON) -m mypy smongo/ web_app.py
