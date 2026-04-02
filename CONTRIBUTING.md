# Contributing to smongo

Thank you for your interest in contributing to smongo! This document covers the workflow and standards for contributing.

## Development Setup

```bash
git clone https://github.com/smongo/smongo.git
cd smongo
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

## Code Style

- **Formatter/Linter**: [ruff](https://docs.astral.sh/ruff/) -- config lives in `pyproject.toml`
- **Type checker**: [mypy](https://mypy-lang.org/) in strict mode
- **Naming**: PEP 8 conventions. Private helpers prefixed with `_`.
- **Docstrings**: Required on all public classes and functions. Use the imperative mood ("Return..." not "Returns...").
- **Comments**: Only explain *why*, never narrate *what*.

## Architecture Quick Reference

| Module | Purpose |
|--------|---------|
| `smongo/storage/` | WiredTiger storage engine: `LocalCollection`, `LocalDB`, `StreamingCursor` |
| `smongo/query/` | MQL query compiler, update operators, expression engine |
| `smongo/aggregation/` | Aggregation pipeline (25+ stages), `Cursor` (lazy `Iterable` input) |
| `smongo/index.py` | B-Tree/text/hashed/wildcard indexes, query planner |
| `smongo/client.py` | `MongoClient`, `Database`, `Collection` -- the public API |
| `smongo/wire/` | MongoDB wire protocol server (OP_MSG), 80+ commands |
| `smongo/sync.py` | Bidirectional Atlas sync |
| `smongo/oplog.py` | Oplog writer, reader, change streams |
| `smongo/schema.py` | `$jsonSchema` document validation |

## Running Tests

```bash
# Full suite
make test

# Single file
pytest tests/test_query.py -v

# With coverage
pytest --cov=smongo --cov-report=term-missing
```

All tests must pass before a PR can be merged. Target: 100% of new code covered.

## Pull Request Process

1. Fork the repo and create a feature branch from `main`
2. Write tests for any new functionality
3. Run `make lint` and `make typecheck` -- both must pass cleanly
4. Run the full test suite with `make test`
5. Write a clear PR description explaining *what* and *why*
6. Request review from a maintainer

## Adding a New Query/Update Operator

1. Implement in `smongo/query/compiler.py` (`_eval_op` for query ops) or `smongo/query/update.py` (`apply_update` for update ops)
2. Add tests in `tests/test_query.py`
3. If the operator is also an aggregation expression, add it to `smongo/query/expressions.py` (`resolve_expr` / `_eval_expr_op`)

## Adding a New Aggregation Stage

1. Implement the stage function in `smongo/aggregation/stages.py` (core stages), `smongo/aggregation/joins.py` (join stages), or `smongo/aggregation/output.py` (terminal stages)
2. Wire it into `Cursor.aggregate`'s dispatch in `smongo/aggregation/cursor.py`
3. Add tests in `tests/test_aggregation.py`

## Adding a New Wire Protocol Command

1. Add a handler in the appropriate `smongo/wire/commands/` sub-module using the `@_register` decorator
2. Add tests in `tests/test_wire_commands.py`

## Commit Messages

Use the imperative mood: "Add $bucket stage" not "Added $bucket stage".

Keep the first line under 72 characters. Add a body if the change needs explanation.

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
