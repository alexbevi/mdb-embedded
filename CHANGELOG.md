# Changelog

Notable changes to **smongo** are recorded here. Earlier history lives in git.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Documentation and internal naming aligned on **smongo-engine + redb** as the embedded storage story (no legacy engine references in tests or docs).

## [0.9.3] — 2026-04-07

Baseline for this changelog file. **smongo** is a PyMongo-style API over **redb** (Rust **smongo-engine**), optional **MongoDB wire protocol**, Atlas **sync**, and related tooling. See **README.md** and **ARCHITECTURE.md** for the current design.
