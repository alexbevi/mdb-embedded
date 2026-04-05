# WiredTiger FFI Strategy

This document describes the linking challenge for calling WiredTiger's C API
from Rust and the strategy for solving it in Phase 1b.

## Current situation

WiredTiger is consumed via `pip install wiredtiger`, which ships a SWIG-generated
Python extension (`_wiredtiger.cpython-311-darwin.so`).  WiredTiger's C library is
**statically linked** into this `.so` — there is no separate `libwiredtiger.dylib`
or `libwiredtiger.a` anywhere in the Python package:

```
$ otool -L .venv/lib/python3.11/site-packages/_wiredtiger.cpython-*.so
  /usr/lib/libSystem.B.dylib
  /usr/lib/libc++.1.dylib
```

All WT symbols (e.g. `wiredtiger_open`, `__wt_cursor_*`) are present in the
extension's symbol table, but they are not exported for other consumers to link
against at build time.

## Why this matters

To call WiredTiger from Rust we need one of:

1. **A linkable library** (`.dylib` / `.a`) resolved at compile time.
2. **Runtime symbol loading** (`dlopen` + `dlsym`).
3. **Going through the Python SWIG layer** (calling Python from Rust via PyO3).

## Options evaluated

### Option A: Build WiredTiger from source (recommended for Phase 1b)

- Clone the [WiredTiger repo](https://github.com/wiredtiger/wiredtiger) at a
  version matching the pip package (currently 11.3.1).
- Build as a static or shared library (`cmake -DENABLE_SHARED=ON`).
- Use `bindgen` with the vendored `wiredtiger.h` header to generate exact Rust
  bindings at build time.
- Link the Rust cdylib against the resulting `libwiredtiger.{a,dylib}`.

**Pros:** Full type safety, zero overhead, enables bypassing the Python SWIG
layer entirely.

**Cons:** Adds a C/C++ build dependency and ~30s to CI.  Must pin the WT version
to match the pip package to avoid ABI drift.

### Option B: `dlopen` the SWIG extension at runtime

- At Python import time, locate `_wiredtiger.cpython-*.so` on disk.
- Use `libloading` (Rust) to `dlopen` it and resolve `wiredtiger_open` by name.
- Call through the resolved function pointer.

**Pros:** No build-time C dependency.  Uses the exact same WT binary Python uses.

**Cons:** Only exported symbols are reachable.  Many internal `__wt_*` helpers
are not exported.  Symbol resolution is fragile across platforms.  Must handle
`dlopen` failures gracefully.  The SWIG extension may have initialization side
effects.

### Option C: Stay above SWIG (call Python WT objects from Rust)

- Use PyO3 to call `wiredtiger.wiredtiger_open(...)` etc. from Rust, operating
  on Python objects.
- Avoids FFI entirely; WT calls go through Python → SWIG → C.

**Pros:** Zero linking complexity.  Works everywhere the Python package works.

**Cons:** No performance gain for WT I/O — the Python/C boundary and GIL
overhead remain.  Only useful as a transitional approach.

## Recommendation

**Phase 1b should pursue Option A** (build from source) because:

1. The goal is to eventually move the entire storage path to Rust, eliminating
   the Python/SWIG layer entirely.
2. `bindgen` generates complete, correct FFI bindings from `wiredtiger.h`.
3. Static linking avoids runtime symbol issues.
4. The WT build is reproducible and can be cached in CI.

Option B (`dlopen`) is the fallback if Option A proves impractical for
distribution (e.g. wheels for platforms where building WT is hard).

## What exists today

- `rust/src/wt_ffi.rs` — Hand-written FFI declarations for the ~20 WT
  functions smongo uses: `wiredtiger_open`, connection/session/cursor vtable
  methods.  These are **not linked** and serve as documentation and scaffolding.
  When `bindgen` is integrated, this file will be replaced by auto-generated
  bindings.

## WT API surface smongo uses

| Object         | Methods                                                                                                 |
| -------------- | ------------------------------------------------------------------------------------------------------- |
| Top-level      | `wiredtiger_open`                                                                                       |
| `WT_CONNECTION` | `open_session`, `close`                                                                                |
| `WT_SESSION`   | `open_cursor`, `create`, `close`, `checkpoint`, `begin_transaction`, `commit_transaction`, `rollback_transaction`, `drop`, `verify`, `compact` |
| `WT_CURSOR`    | `set_key`, `set_value`, `get_key`, `get_value`, `search`, `search_near`, `insert`, `update`, `remove`, `next`, `reset`, `close` |

Config strings, error codes, and URI conventions are documented inline in
`wt_ffi.rs`.

## Open questions for Phase 1b

1. **WT version pinning**: How to ensure the Rust-linked WT version matches the
   pip package.  Likely: pin both to the same git tag.
2. **Dual-database risk**: If Rust opens its own `WT_CONNECTION` on the same
   data directory while Python has one open, WT will fail with `EBUSY`.  The
   migration must be all-or-nothing per connection.
3. **Cross-platform builds**: macOS (arm64, x86_64), Linux (x86_64, aarch64).
   WT builds cleanly on all via cmake.
4. **WASM target**: WT is C and not trivially compilable to WASM.  The WASM
   target (Phase 7 in `FUTURE_PLANS.md`) will likely need an alternative storage
   backend.
