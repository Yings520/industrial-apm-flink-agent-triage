from pathlib import Path


LOADER = Path("scripts/phase4-load-incidents.py").read_text()


def test_phase4_loader_has_clickhouse_text_casts() -> None:
    assert "USE_CLICKHOUSE" in LOADER
    assert "toString({column})" in LOADER
    assert "CAST({column} AS CHAR)" in LOADER


def test_phase4_loader_normalizes_datetime_literals() -> None:
    assert "_datetime_sql" in LOADER
    assert ".replace(\"T\", \" \")" in LOADER
    assert ".removesuffix(\"Z\")" in LOADER
