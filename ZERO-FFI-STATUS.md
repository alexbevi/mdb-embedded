# Zero-FFI Aggregation: Status

## COMPLETE

All 27 aggregation stages run in the Rust engine with zero FFI round-trips.
All entry points (Python API, Python wire, Rust wire) route through the engine.
`PYTHON_REQUIRED_STAGES` is empty. Zero warnings in `cargo check`.

### Architecture

```
 Collection.aggregate()  -->  _rust_coll.aggregate_engine()  --\
 Python Wire Handler     -->  _rust_coll.aggregate_engine()  ---+--> ONE FFI crossing
 Rust Wire Handler       -->  aggregate_pipeline(db_handle)  --/          |
                                                                          v
                                                              smongo-engine (pure Rust)
                                                              27 stages, 80+ expressions,
                                                              18 accumulators, DatabaseContext
```

### Pipeline Conversion

`pylist_to_pipeline()` converts Python dicts to validated BSON:
- `pyany_to_doc()` handles dict subclasses (`bson.SON`, `OrderedDict`) via Python `.items()` protocol
- Each stage validated: non-empty, `$`-prefixed operator key
- Diagnostic errors include stage index, invalid key, all keys found

### Files

| File | Role |
|------|------|
| `rust/smongo-py/src/bson_helpers.rs` | `pyany_to_doc()`, `pylist_to_pipeline()` |
| `rust/smongo-py/src/redb_client.rs` | `aggregate_engine()` entry point |
| `rust/smongo-py/src/aggregation.rs` | `aggregate_pipeline()` -> engine dispatch |
| `rust/smongo-py/src/wire_commands/aggregate.rs` | Rust wire -> engine via `db_handle` |
| `smongo/wire/commands/aggregation.py` | Python wire -> `aggregate_engine()` |
| `rust/smongo-engine/src/aggregation/` | All 27 stage implementations |

### Verification

```bash
cargo check --manifest-path=rust/Cargo.toml          # 0 warnings
cargo test --manifest-path=rust/Cargo.toml            # 364 tests, 0 failures
cargo test --package smongo-engine aggregation \
    --manifest-path=rust/Cargo.toml                   # 74/74 engine tests
```
