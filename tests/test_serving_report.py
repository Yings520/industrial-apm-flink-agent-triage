from pathlib import Path

from src.serving.report import SECTION_HEADINGS, render_report, sample_results, write_report


def test_report_contains_required_sections_and_local_label(tmp_path: Path) -> None:
    report = render_report(sample_results())
    for heading in SECTION_HEADINGS:
        assert f"## {heading}" in report
    assert "local/synthetic demo metrics" in report
    assert "dwd_apm_sensor_readings_rt" in report
    output = write_report(sample_results(), tmp_path / "phase3-serving-report.md")
    assert output.read_text().startswith("# Phase 3 Serving Report")
