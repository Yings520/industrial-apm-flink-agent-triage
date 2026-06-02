from pathlib import Path


def test_readme_exists() -> None:
    assert Path("README.md").exists()


def test_pyproject_exists() -> None:
    assert Path("pyproject.toml").exists()


def test_schema_exists() -> None:
    assert Path("sql/clickhouse/ddl/schema.sql").exists()


def test_python_package_importable() -> None:
    import src.streaming.topic_names  # noqa: F401
