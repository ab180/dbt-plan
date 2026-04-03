"""Project configuration from .dbt-plan.yml."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

try:
    import yaml as _yaml  # noqa: F401

    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False


@dataclass
class Config:
    """dbt-plan project configuration."""

    dbt_cmd: str = "dbt"
    target_dir: str = "target"
    base_dir: str = ".dbt-plan/base"
    profiles_dir: str | None = None

    @classmethod
    def load(cls, project_dir: str | Path = ".") -> Config:
        """Load config from .dbt-plan.yml in project_dir.

        Falls back to defaults if file doesn't exist or pyyaml isn't installed.
        """
        config_path = Path(project_dir) / ".dbt-plan.yml"
        if not config_path.exists():
            return cls()

        if not _HAS_YAML:
            # pyyaml not installed — parse simple key: value lines
            return cls._parse_simple(config_path)

        import yaml

        data = yaml.safe_load(config_path.read_text()) or {}
        return cls._from_dict(data)

    @classmethod
    def _from_dict(cls, data: dict) -> Config:
        return cls(
            dbt_cmd=data.get("dbt_cmd", "dbt"),
            target_dir=data.get("target_dir", "target"),
            base_dir=data.get("base_dir", ".dbt-plan/base"),
            profiles_dir=data.get("profiles_dir"),
        )

    @classmethod
    def _parse_simple(cls, path: Path) -> Config:
        """Minimal YAML-like parser for key: value lines (no pyyaml needed)."""
        data: dict[str, str] = {}
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                key, _, value = line.partition(":")
                data[key.strip()] = value.strip().strip("'\"")
        return cls._from_dict(data)
