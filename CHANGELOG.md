# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-03

### Added
- `dbt-plan check` command: diff compiled SQL and predict DDL impact
- `dbt-plan snapshot` command: save baseline compiled state + manifest
- SQLGlot-based column extraction (Snowflake dialect)
- DDL prediction rules for all materialization x on_schema_change combinations
- Downstream impact discovery via manifest child_map (BFS with cycle protection)
- Text and GitHub markdown output formats
- Exit codes: 0 (safe), 1 (destructive), 2 (parse error/warning)
- Parse failure safety: never returns SAFE when columns cannot be extracted
- SELECT * detection: returns WARNING instead of false predictions
- Removed model detection: DESTRUCTIVE regardless of materialization
- Base manifest fallback: finds removed models in snapshot manifest

### Safety guarantees
- Parse failure on sync_all_columns model -> WARNING (never SAFE)
- SELECT * wildcard -> WARNING (cannot determine column diff)
- Removed model -> DESTRUCTIVE (MODEL REMOVED operation)

[Unreleased]: https://github.com/ab180/dbt-plan/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ab180/dbt-plan/releases/tag/v0.1.0
