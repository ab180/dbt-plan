# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Cascade impact analysis**: detect downstream broken column references and build failures
  - `BROKEN_REF`: downstream SQL references a dropped column (word-boundary matching)
  - `BUILD_FAILURE`: downstream incremental with `on_schema_change=fail`
  - Safety escalation: cascade risks affect exit code (broken_ref → DESTRUCTIVE)
  - Shown in all output formats (text, github, json)

## [0.2.0] - 2026-04-06

### Added
- **Config system**: `.dbt-plan.yml` + 7 environment variables (`DBT_PLAN_*`)
- **New commands**: `dbt-plan init` (generates config + updates .gitignore), `dbt-plan stats` (project analysis)
- **New flags**: `--format json`, `--select` / `-s`, `--verbose` / `-v`, `--no-color`, `--dialect`
- **Manifest column fallback**: resolves SELECT * models using dbt column definitions
- **Package model filtering**: auto-excludes dbt package models
- **Multi-dialect support**: snowflake, bigquery, postgres, mysql, duckdb, trino
- **CI workflow template**: `examples/ci-workflow/dbt-plan.yml` with PR comment posting
- **CI summary line**: grepable `dbt-plan: N checked, X safe, Y warning, Z destructive`
- **ANSI color output**: red/yellow/green safety labels (auto-disabled when piped)
- **PyPI publishing**: `pip install dbt-plan`

### Fixed
- Qualified star (`SELECT t.*`) correctly returns `["*"]`
- Removed ephemeral models return SAFE (no physical object)
- Snapshot materialization handled explicitly (WARNING)
- Column reordering detected for `sync_all_columns`
- Stale column warnings for `append_new_columns`
- Unaliased expression ambiguity detection
- Flat compiled dir layout support
- Multi-project dir safety (ValueError on ambiguity)
- Actionable error messages
- Exit code 0 for no-args help

### Performance
- O(1) node index, streaming manifest, SQL caching, lazy imports
- Memoized batch downstream BFS, file-size fast path

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

[Unreleased]: https://github.com/ab180/dbt-plan/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ab180/dbt-plan/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ab180/dbt-plan/releases/tag/v0.1.0
