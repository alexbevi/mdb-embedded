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
| `rust/` | **Engine + bindings.** `smongo-engine` (redb, WASM backends), `smongo-py` (`RedbLocalClient`, `RedbLocalCollection`, wire handlers, Tokio TCP server). |
| `smongo/_smongo_core` | Compiled Rust extension (PyO3) — built via maturin from `rust/smongo-py` |
| `smongo/client.py` | `MongoClient`, `Database`, `Collection`. Routes `local://` to `RedbClient` / `RedbCollection`. |
| `smongo/storage/` | `redb_engine` (`RedbClient`, `RedbCollection`), `TTLReaper`, result types, locking, transaction session, BSON helpers |
| `smongo/query/` | MQL compiler, update operators, expression engine (Rust-accelerated) |
| `smongo/aggregation/` | Aggregation pipeline (25+ stages, Rust-accelerated), `Cursor` (Python lazy wrapper) |
| `smongo/index.py` | Index key encoding, helpers, `DuplicateKeyError`; legacy `IndexManager` for tooling; runtime indexes live in the engine |
| `smongo/wire/` | MongoDB wire protocol server (OP_MSG), 80+ commands (Rust-accelerated) |
| `smongo/sync.py` | Bidirectional Atlas sync with MQL rules, variable substitution, vector clocks |
| `smongo/oplog.py` | Oplog writer, reader, change streams |
| `smongo/schema.py` | `$jsonSchema` document validation |

## Running Tests

```bash
# Rebuild the PyO3 extension after Rust changes (fast iteration)
make build-debug

# Python unit tests (no Docker, no network)
make test

# Rust workspace tests + clippy (same as part of `make build-rust`)
make test-rust

# Static checks: ruff, mypy, Rust tests + clippy
make check

# Integration tests (needs Docker — starts real MongoDB; see tests/integration)
make test-integration

# Everything CI-like: Rust + unit + integration
make test-all

# Single file
pytest tests/test_query.py -v

# With coverage
pytest --cov=smongo --cov-report=term-missing
```

### Verifying a clean tree

From a fresh clone, a typical full pass is:

1. `pip install -e ".[dev]"` (or `make install-dev`) and ensure `maturin` can build the extension.
2. `make check` — lint, types, Rust tests + clippy.
3. `make test` — full `pytest tests`.
4. With Docker available: `make test-integration`.

If `pytest` fails with import errors from `_smongo_core`, run `make build-debug` or `pip install -e .` again from the repo root.

All tests must pass before a PR can be merged. Target: 100% of new code covered.

## Pull Request Process

1. Fork the repo and create a feature branch from `main`
2. Write tests for any new functionality
3. Run `make lint` and `make typecheck` -- both must pass cleanly
4. Run the full test suite with `make test`
5. Write a clear PR description explaining *what* and *why*
6. Request review from a maintainer

## Adding a New Query/Update Operator

1. Implement in `rust/smongo-py/src/query_compiler.rs` (`eval_query` for query ops) or `rust/smongo-py/src/query_update.rs` (`apply_update` for update ops). The Python modules in `smongo/query/` are thin shims that delegate to the Rust extension.
2. Add tests in `tests/test_query.py`
3. If the operator is also an aggregation expression, add it to `rust/smongo-py/src/query_expressions.rs` (`eval_expr_op`)

## Adding a New Aggregation Stage

All 27 pipeline stages now run in the Rust engine (`smongo-engine`). To add a new stage:

1. Implement the stage in `rust/smongo-engine/src/aggregation/stages.rs` (or a new sub-module)
2. Wire it into the pipeline dispatch in `rust/smongo-engine/src/aggregation/mod.rs`
3. If the stage needs PyO3 bridging, update `rust/smongo-py/src/aggregation.rs`
4. Update the Python reference in `smongo/aggregation/stages.py` (or `joins.py` / `output.py`) for documentation parity
5. Add tests in `tests/test_aggregation.py` and `rust/smongo-engine/src/aggregation/` (Rust unit tests)

## Adding a New Wire Protocol Command

**Python fallback path:** Add a handler in `smongo/wire/commands/` using the `@_register` decorator. The Python `WireServer` dispatches via the `_HANDLERS` dict.

**Rust hot path:** Add a handler function in the appropriate `rust/smongo-py/src/wire_commands/` module (e.g. `crud.rs`, `admin.rs`). The handler must match the `HandlerFn` signature:

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
