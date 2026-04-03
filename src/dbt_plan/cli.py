"""CLI entry point for dbt-plan."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from dbt_plan.columns import extract_columns
from dbt_plan.config import Config
from dbt_plan.diff import diff_compiled_dirs
from dbt_plan.formatter import CheckResult, format_github, format_text
from dbt_plan.manifest import find_downstream, find_node_by_name, load_manifest
from dbt_plan.predictor import Safety, predict_ddl


def _find_compiled_dir(target_dir: Path) -> Path | None:
    """Find compiled SQL models directory inside target/.

    dbt puts compiled SQL at target/compiled/{project_name}/models/.
    Scans for the first matching subdirectory.
    """
    compiled = target_dir / "compiled"
    if not compiled.exists():
        return None
    for project_dir in compiled.iterdir():
        if project_dir.is_dir():
            models_dir = project_dir / "models"
            if models_dir.exists():
                return models_dir
    return None


def _do_snapshot(args: argparse.Namespace) -> None:
    """Save current compiled state as baseline (compiled SQL + manifest)."""
    project_dir = Path(args.project_dir)
    target_dir = project_dir / args.target_dir
    base_dir = project_dir / ".dbt-plan" / "base"

    compiled_dir = _find_compiled_dir(target_dir)
    if compiled_dir is None:
        print(
            f"Error: No compiled SQL found in {target_dir}/compiled/*/models/",
            file=sys.stderr,
        )
        sys.exit(2)

    if base_dir.exists():
        shutil.rmtree(base_dir)

    # Save compiled SQL
    compiled_dest = base_dir / "compiled"
    shutil.copytree(compiled_dir, compiled_dest)

    # Save manifest.json alongside compiled SQL
    manifest_src = target_dir / "manifest.json"
    if manifest_src.exists():
        shutil.copy2(manifest_src, base_dir / "manifest.json")

    print(f"Snapshot saved to {base_dir}")


def _do_check(args: argparse.Namespace) -> int:
    """Diff compiled SQL and predict DDL impact.

    Returns:
        Exit code: 0=safe, 1=destructive, 2=error.
    """
    project_dir = Path(args.project_dir)
    target_dir = project_dir / args.target_dir
    base_dir = project_dir / Path(args.base_dir)
    manifest_path = Path(
        args.manifest if args.manifest else str(target_dir / "manifest.json")
    )

    # Validate paths
    if not base_dir.exists():
        print(
            f"Error: Base directory not found: {base_dir}. Run 'dbt-plan snapshot' first.",
            file=sys.stderr,
        )
        return 2

    # Resolve compiled SQL directories
    base_compiled = base_dir / "compiled"
    if not base_compiled.exists():
        # Backward compat: old snapshot format stored SQL directly in base_dir
        base_compiled = base_dir

    current_compiled = _find_compiled_dir(target_dir)
    if current_compiled is None:
        print(
            f"Error: No compiled SQL found in {target_dir}/compiled/*/models/",
            file=sys.stderr,
        )
        return 2

    if not manifest_path.exists():
        print(f"Error: manifest.json not found: {manifest_path}", file=sys.stderr)
        return 2

    # 1. Diff compiled dirs
    model_diffs = diff_compiled_dirs(base_compiled, current_compiled)
    if not model_diffs:
        fmt = format_text if args.format == "text" else format_github
        print(fmt(CheckResult()))
        return 0

    # 2. Load manifests (current + base for removed model fallback)
    try:
        manifest = load_manifest(manifest_path)
    except (json.JSONDecodeError, OSError) as e:
        print(f"Error: Could not parse manifest.json: {e}", file=sys.stderr)
        return 2

    base_manifest_path = base_dir / "manifest.json"
    base_manifest = None
    if base_manifest_path.exists():
        try:
            base_manifest = load_manifest(base_manifest_path)
        except (json.JSONDecodeError, OSError):
            pass  # base manifest is best-effort

    child_map = manifest.get("child_map", {})
    if base_manifest:
        # Merge base child_map for removed models
        for k, v in base_manifest.get("child_map", {}).items():
            if k not in child_map:
                child_map[k] = v

    # 3. For each changed model: extract columns, predict DDL
    predictions = []
    parse_failures: list[str] = []
    downstream_map: dict[str, list[str]] = {}

    for diff in model_diffs:
        # Look up in current manifest first, fall back to base manifest for removed models
        node = find_node_by_name(diff.model_name, manifest)
        if node is None and base_manifest is not None:
            node = find_node_by_name(diff.model_name, base_manifest)
        if node is None:
            continue  # ephemeral or unknown in both manifests

        base_cols = None
        current_cols = None
        if diff.base_path:
            base_cols = extract_columns(diff.base_path.read_text())
        if diff.current_path:
            current_cols = extract_columns(diff.current_path.read_text())

        # Track parse failures only for models where it matters
        # (table/view are always safe via CREATE OR REPLACE, so parse failure is irrelevant)
        if (
            diff.status == "modified"
            and node.materialization not in ("table", "view")
            and (base_cols is None or current_cols is None)
        ):
            parse_failures.append(diff.model_name)

        prediction = predict_ddl(
            model_name=diff.model_name,
            materialization=node.materialization,
            on_schema_change=node.on_schema_change,
            base_columns=base_cols,
            current_columns=current_cols,
            status=diff.status,
        )
        predictions.append(prediction)

        downstream = find_downstream(node.node_id, child_map)
        if downstream:
            downstream_names = [nid.split(".")[-1] for nid in downstream]
            downstream_map[diff.model_name] = downstream_names

    # 4. Format output
    check_result = CheckResult(predictions, downstream_map, parse_failures)
    if args.format == "github":
        print(format_github(check_result))
    else:
        print(format_text(check_result))

    # 5. Exit code
    if any(p.safety == Safety.DESTRUCTIVE for p in predictions):
        return 1
    if parse_failures:
        return 2
    return 0


def _do_init(args: argparse.Namespace) -> None:
    """Create a .dbt-plan.yml config file."""
    project_dir = Path(args.project_dir)
    config_path = project_dir / ".dbt-plan.yml"
    if config_path.exists():
        print(f"Config already exists: {config_path}")
        return

    config_path.write_text(
        "# dbt-plan configuration\n"
        "# See: https://github.com/ab180/dbt-plan/blob/main/docs/configuration.md\n"
        "\n"
        "# dbt execution command (default: dbt)\n"
        "# Examples: uv run dbt, poetry run dbt, pipx run dbt\n"
        "dbt_cmd: dbt\n"
        "\n"
        "# dbt compile output directory (default: target)\n"
        "target_dir: target\n"
        "\n"
        "# Snapshot baseline directory (default: .dbt-plan/base)\n"
        "base_dir: .dbt-plan/base\n"
        "\n"
        "# dbt profiles directory (default: none, uses dbt default)\n"
        "# profiles_dir: ./profiles\n"
    )
    print(f"Config created: {config_path}")


def main() -> None:
    from dbt_plan import __version__

    # Pre-parse to find --project-dir for config loading
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--project-dir", default=".")
    pre_args, _ = pre_parser.parse_known_args()
    config = Config.load(pre_args.project_dir)

    parser = argparse.ArgumentParser(
        prog="dbt-plan",
        description="Preview what DDL changes dbt run will execute on Snowflake",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    subparsers = parser.add_subparsers(dest="command")

    # init
    init_cmd = subparsers.add_parser(
        "init", help="Create .dbt-plan.yml config file"
    )
    init_cmd.add_argument(
        "--project-dir", default=".", help="dbt project directory (default: .)"
    )

    # snapshot
    snap = subparsers.add_parser(
        "snapshot", help="Save current compiled state as baseline"
    )
    snap.add_argument(
        "--project-dir", default=".", help="dbt project directory (default: .)"
    )
    snap.add_argument(
        "--target-dir", default=config.target_dir,
        help=f"dbt target directory (default: {config.target_dir})",
    )

    # check
    check = subparsers.add_parser(
        "check", help="Diff compiled SQL and predict DDL impact"
    )
    check.add_argument(
        "--project-dir", default=".", help="dbt project directory (default: .)"
    )
    check.add_argument(
        "--target-dir", default=config.target_dir,
        help=f"dbt target directory (default: {config.target_dir})",
    )
    check.add_argument(
        "--base-dir", default=config.base_dir,
        help=f"Baseline snapshot directory (default: {config.base_dir})",
    )
    check.add_argument(
        "--manifest",
        default=None,
        help="Path to manifest.json (default: {target-dir}/manifest.json)",
    )
    check.add_argument(
        "--format",
        choices=["text", "github"],
        default="text",
        help="Output format (default: text)",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "init":
        _do_init(args)

    if args.command == "snapshot":
        _do_snapshot(args)

    if args.command == "check":
        sys.exit(_do_check(args))


if __name__ == "__main__":
    main()
