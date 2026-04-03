# Releasing

## Versioning

[Semantic Versioning](https://semver.org/)을 따릅니다.

- **MAJOR** (1.0.0): 호환성 깨지는 변경 (CLI 인터페이스, 출력 포맷, exit code)
- **MINOR** (0.2.0): 새 기능 추가 (하위 호환)
- **PATCH** (0.1.1): 버그 수정

## Release Checklist

1. `pyproject.toml`의 `version` 업데이트
2. `src/dbt_plan/__init__.py`의 `__version__` 업데이트
3. `CHANGELOG.md`에 릴리즈 항목 추가
4. 커밋: `git commit -m "release: v0.x.0"`
5. 태그: `git tag v0.x.0`
6. 푸시: `git push && git push --tags`
7. GitHub Release가 자동 생성됨 (`.github/workflows/release.yml`)

## Automation

`v*` 태그를 푸시하면 GitHub Actions가 자동으로:
- Python wheel 빌드 (`python -m build`)
- GitHub Release 생성 + dist 첨부
- Release notes 자동 생성 (`generate_release_notes: true`)

## Installing a Release

```bash
# 특정 버전
pip install git+https://github.com/ab180/dbt-plan@v0.1.0

# 최신
pip install git+https://github.com/ab180/dbt-plan
```

## PyPI (미정)

현재 PyPI에 게시하지 않습니다. GitHub release로 배포합니다.
향후 `pip install dbt-plan`이 필요해지면 PyPI 게시를 추가합니다.
