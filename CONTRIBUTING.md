# Contributing to smongo

Thank you for your interest in contributing to smongo! This document covers the workflow and standards for contributing.

## Development Setup

```bash
git clone https://github.com/ranfysvalle02/mdb-embedded.git
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
| `rust/` | **The engine.** `RustLocalClient`, `RustLocalDB`, `RustLocalCollection`, `RustIndexManager`, `RustQueryPlanner`, `RustStreamingCursor`, wire command handlers, Tokio TCP server. Direct WiredTiger C FFI. |
| `smongo/_smongo_core` | Compiled Rust extension (PyO3) -- built from `rust/` via maturin |
| `smongo/client.py` | `MongoClient`, `Database`, `Collection` -- the public API. Routes `local://` to `LocalClient`. |
| `smongo/storage/` | Storage layer: `LocalClient`/`LocalDB` (Python, delegates to Rust), `TTLReaper`, result types, locking, transaction session, BSON helpers |
| `smongo/query/` | MQL compiler, update operators, expression engine (Rust-accelerated) |
| `smongo/aggregation/` | Aggregation pipeline (25+ stages, Rust-accelerated), `Cursor` (Python lazy wrapper) |
| `smongo/index.py` | Index key encoding, helpers, `DuplicateKeyError` (`IndexManager`/`QueryPlanner` classes removed; runtime: `RustIndexManager`, `RustQueryPlanner`) |
| `smongo/wire/` | MongoDB wire protocol server (OP_MSG), 80+ commands (Rust-accelerated) |
| `smongo/sync.py` | Bidirectional Atlas sync with MQL rules, variable substitution, vector clocks |
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

1. Implement in `rust/src/query_compiler.rs` (`eval_query` for query ops) or `rust/src/query_update.rs` (`apply_update` for update ops). The Python modules in `smongo/query/` are thin shims that delegate to the Rust extension.
2. Add tests in `tests/test_query.py`
3. If the operator is also an aggregation expression, add it to `rust/src/query_expressions.rs` (`eval_expr_op`)

## Adding a New Aggregation Stage

1. Implement the stage function in `smongo/aggregation/stages.py` (core stages), `smongo/aggregation/joins.py` (join stages), or `smongo/aggregation/output.py` (terminal stages)
2. Wire it into `Cursor.aggregate`'s dispatch in `smongo/aggregation/cursor.py`
3. Add tests in `tests/test_aggregation.py`

## Adding a New Wire Protocol Command

**Python fallback path:** Add a handler in `smongo/wire/commands/` using the `@_register` decorator. The Python `WireServer` dispatches via the `_HANDLERS` dict.

**Rust hot path:** Add a handler function in the appropriate `rust/src/wire_commands/` module (e.g. `crud.rs`, `admin.rs`). The handler must match the `HandlerFn` signature:

```rust
fn cmd_my_command(
    py: Python<'_>,
    ctx: &Bound<'_, ConnectionContext>,  // typed, not erased
    cmd: &Bound<'_, PyDict>,
    seqs: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> { ... }
```

Register it in the module's `register()` function. `rs_dispatch` performs a single downcast of `ctx` and routes to the handler via `RUST_HANDLERS`. Field access on `ConnectionContext` is direct (no `getattr`). Frequently-used Python modules are cached via `CachedImports` (Arc-shared, per-connection) and `cached_modules` (`PyOnceLock`, per-process — free-threading-safe).

Add tests in `tests/test_wire_commands.py` or the appropriate existing test file.

## Commit Messages

Use the imperative mood: "Add $bucket stage" not "Added $bucket stage".

Keep the first line under 72 characters. Add a body if the change needs explanation.

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
