import re
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

EXPECTED_DIM_TABLES = [
    "dim_apm_assets",
    "dim_apm_asset_components",
    "dim_apm_signal_tags_mapping",
    "dim_apm_failuremode",
    "dim_apm_failureevent",
    "dim_apm_workorder",
    "dim_apm_maintenance",
    "dim_apm_metrics",
    "dim_apm_sites",
    "dim_apm_threshold_profiles",
    "dim_apm_measurement_points",
    "dim_apm_tenants",
]

EXPECTED_DWD_TABLES = [
    "dwd_apm_realtime_signal_readings",
    "dwd_apm_anomaly_events",
    "dwd_apm_late_sensor_readings",
    "dwd_apm_stream_health_snapshots",
    "dwd_apm_realtime_diagnosis_events",
    "dwd_apm_dlq_events",
]

EXPECTED_ADS_TABLES = [
    "ads_apm_asset_condition_snapshot",
    "ads_apm_asset_condition_timeseries_1m",
    "ads_apm_tag_signal_timeseries_1m",
    "ads_apm_anomaly_event_realtime",
    "ads_apm_diagnosis_kpi_snapshot",
    "ads_apm_stream_health_snapshot",
    "ads_apm_failure_evidence_summary",
]

EXPECTED_CDC_FIELDS = {"cdc_op", "cdc_updated_at", "doris_loaded_at"}

EXPECTED_ROUTINE_LOAD_TARGETS = [
    "dim_apm_assets",
    "dim_apm_asset_components",
    "dim_apm_signal_tags_mapping",
    "dim_apm_failuremode",
    "dim_apm_failureevent",
    "dim_apm_workorder",
    "dim_apm_maintenance",
    "dim_apm_tenants",
    "dwd_apm_realtime_signal_readings",
    "dwd_apm_anomaly_events",
    "dwd_apm_late_sensor_readings",
    "dwd_apm_stream_health_snapshots",
    "dwd_apm_realtime_diagnosis_events",
    "dwd_apm_dlq_events",
]


def parse_doris_schema():
    schema_path = PROJECT_ROOT / "sql" / "doris" / "ddl" / "schema.sql"
    if not schema_path.exists():
        pytest.skip("sql/doris/ddl/schema.sql not found")

    content = schema_path.read_text()
    table_blocks = re.split(r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+', content, flags=re.IGNORECASE)

    tables = {}
    for block in table_blocks[1:]:
        table_name = block.strip().split()[0]

        key_match = re.search(
            r'\)\n\s*(DUPLICATE\s+KEY|UNIQUE\s+KEY)\s*\(([^)]+)\)',
            block, re.IGNORECASE
        )
        if not key_match:
            continue

        key_type = key_match.group(1).strip()
        key_columns = [k.strip() for k in key_match.group(2).split(",")]

        paren_start = block.index("(")
        col_end = key_match.start()
        col_section = block[paren_start + 1 : col_end]

        columns = set()
        for line in col_section.splitlines():
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            parts = line.split()
            if parts:
                columns.add(parts[0].rstrip(","))

        tables[table_name] = {
            "columns": columns,
            "key_type": key_type,
            "key_columns": key_columns,
        }

    return tables


def parse_routine_load():
    rl_path = PROJECT_ROOT / "sql" / "doris" / "templates" / "routine_load.sql.tpl"
    if not rl_path.exists():
        pytest.skip("sql/doris/templates/routine_load.sql.tpl not found")

    with open(rl_path) as f:
        content = f.read()

    targets = set()
    on_pattern = re.compile(r'ON\s+(\w+)', re.IGNORECASE)
    for match in on_pattern.finditer(content):
        targets.add(match.group(1))

    return targets


def test_doris_schema_tables_exist():
    tables = parse_doris_schema()

    for table_name in EXPECTED_DIM_TABLES:
        assert table_name in tables, f"DIM table {table_name} missing from sql/doris/ddl/schema.sql"

    for table_name in EXPECTED_DWD_TABLES:
        assert table_name in tables, f"DWD table {table_name} missing from sql/doris/ddl/schema.sql"

    for table_name in EXPECTED_ADS_TABLES:
        assert table_name in tables, f"ADS table {table_name} missing from sql/doris/ddl/schema.sql"


def test_dim_tables_have_cdc_fields():
    tables = parse_doris_schema()

    for table_name in EXPECTED_DIM_TABLES:
        assert table_name in tables, f"DIM table {table_name} not found"
        missing = EXPECTED_CDC_FIELDS - tables[table_name]["columns"]
        assert len(missing) == 0, (
            f"DIM table {table_name} missing CDC fields: {missing}"
        )


def test_dwd_signal_table_is_duplicate_key():
    tables = parse_doris_schema()
    assert "dwd_apm_realtime_signal_readings" in tables
    key_type = tables["dwd_apm_realtime_signal_readings"]["key_type"]
    assert "DUPLICATE" in key_type.upper(), (
        f"Signal readings table should be DUPLICATE KEY, got {key_type}"
    )


def test_routine_loads_cover_all_targets():
    targets = parse_routine_load()

    for table_name in EXPECTED_ROUTINE_LOAD_TARGETS:
        assert table_name in targets, (
            f"No routine load for target table: {table_name}"
        )


def test_ads_tables_have_partitioning():
    schema_path = PROJECT_ROOT / "sql" / "doris" / "ddl" / "schema.sql"
    with open(schema_path) as f:
        content = f.read()

    for table_name in EXPECTED_ADS_TABLES:
        pattern = re.compile(
            rf'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{table_name}[\s\S]*?PARTITION\s+BY\s+RANGE',
            re.IGNORECASE
        )
        assert pattern.search(content), (
            f"ADS table {table_name} missing PARTITION BY RANGE strategy"
        )
