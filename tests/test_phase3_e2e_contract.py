from pathlib import Path


def test_phase3_e2e_contains_visible_data_checks() -> None:
    script = Path("scripts/phase3-e2e.sh").read_text()
    assert "make serving-query" in script
    assert "make phase3-report" in script
    assert "Redpanda Console: http://localhost:18080" in script
    assert "Flink UI: http://localhost:18081" in script
    assert "Doris FE: http://localhost:18030" in script


def test_makefile_exposes_phase3_targets() -> None:
    makefile = Path("Makefile").read_text()
    assert "phase3-e2e:" in makefile
    assert "phase3-report:" in makefile
    assert "serving-query:" in makefile


def test_phase3_contract_mentions_routine_load_and_fallback() -> None:
    routine_sql = Path("sql/doris_routine_load.sql").read_text()
    readme = Path("README.md").read_text()
    assert "CREATE ROUTINE LOAD" in routine_sql
    assert "Routine Load" in readme
    assert "fallback" in readme
    assert "local/synthetic" in readme
