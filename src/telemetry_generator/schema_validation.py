from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from src.telemetry_generator.config import REPO_ROOT

SCHEMA_PATHS = {
    "raw_sensor_event": REPO_ROOT / "contracts" / "raw_sensor_event.schema.json",
    "sensor_event": REPO_ROOT / "contracts" / "sensor_event.schema.json",
}


@dataclass(frozen=True)
class ValidationResult:
    exit_code: int
    accepted_path: Path
    rejected_path: Path
    summary_path: Path
    total_records: int
    accepted_records: int
    rejected_records: int


class ValidationCliNamespace(argparse.Namespace):
    input: Path = Path()
    output_dir: Path = Path()
    profile: str | None = None
    schema: str = "raw_sensor_event"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _load_schema(schema_name: str) -> dict[str, object]:
    if schema_name not in SCHEMA_PATHS:
        raise ValueError(f"Unknown schema: {schema_name}")
    payload = cast(object, json.loads(SCHEMA_PATHS[schema_name].read_text(encoding="utf-8")))
    if not isinstance(payload, dict):
        raise ValueError(f"{schema_name} schema must be a JSON object")
    return cast(dict[str, object], payload)


def _schema_version(schema: dict[str, object]) -> str:
    properties_value = schema.get("properties")
    if not isinstance(properties_value, dict):
        raise ValueError("schema properties object is required")
    properties = cast(dict[str, object], properties_value)
    schema_version_value = properties.get("schema_version")
    if not isinstance(schema_version_value, dict):
        raise ValueError("schema_version schema object is required")
    schema_version = cast(dict[str, object], schema_version_value)
    version = schema_version.get("const")
    if not isinstance(version, str):
        raise ValueError("schema_version const is required")
    return version


def _profile_files(input_dir: Path, profile_name: str, schema_name: str) -> list[Path]:
    if schema_name == "raw_sensor_event":
        return [
            input_dir / f"raw_sensor_events_{profile_name}.jsonl",
            input_dir / f"raw_sensor_events_invalid_{profile_name}.jsonl",
        ]
    return [
        input_dir / f"sensor_events_{profile_name}.jsonl",
        input_dir / f"invalid_events_{profile_name}.jsonl",
    ]


def _jsonl_files(input_dir: Path, profile_name: str | None = None, schema_name: str = "raw_sensor_event") -> list[Path]:
    if profile_name is not None:
        expected = _profile_files(input_dir, profile_name, schema_name)
        missing = [path for path in expected if not path.is_file()]
        if missing:
            missing_paths = ", ".join(str(path) for path in missing)
            raise FileNotFoundError(f"Missing profile input file(s): {missing_paths}")
        return expected
    return sorted(
        path for path in input_dir.glob("*.jsonl") if path.is_file() and not path.name.startswith("validated_")
    )


def _iter_records(
    input_dir: Path,
    profile_name: str | None = None,
    schema_name: str = "raw_sensor_event",
) -> list[tuple[dict[str, object] | None, str | None]]:
    records: list[tuple[dict[str, object] | None, str | None]] = []
    for path in _jsonl_files(input_dir, profile_name, schema_name):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = cast(object, json.loads(line))
            except json.JSONDecodeError as exc:
                records.append((None, f"Invalid JSON: {exc.msg}"))
                continue
            if not isinstance(payload, dict):
                records.append((None, "JSONL record must be an object"))
                continue
            records.append((cast(dict[str, object], payload), None))
    return records


def _missing_required_fields(error: ValidationError) -> list[str]:
    validator_value = error.validator_value
    instance = cast(object, error.instance)
    if error.validator != "required" or not isinstance(instance, dict) or not isinstance(validator_value, list):
        return []
    instance_mapping = cast(dict[str, object], instance)
    required_fields = cast(list[object], validator_value)
    return [field for field in required_fields if isinstance(field, str) and field not in instance_mapping]


def _field_path(error: ValidationError) -> str:
    if error.validator == "required":
        missing = _missing_required_fields(error)
        if missing:
            return f"/{missing[0]}"
    path = "/" + "/".join(str(part) for part in error.absolute_path)
    return path if path != "/" else "/"


def _error_code(error: ValidationError) -> str:
    if error.validator == "required":
        return "missing_required_field"
    if error.validator in {"format", "pattern"}:
        return "invalid_format"
    if error.validator == "additionalProperties":
        return "unexpected_field"
    if error.validator in {"const", "enum", "minimum", "maximum"}:
        return "invalid_value"
    if error.validator == "type":
        return "invalid_type"
    return "schema_validation_error"


def _sort_errors(errors: list[ValidationError]) -> list[ValidationError]:
    return sorted(errors, key=lambda error: (_field_path(error), _error_code(error), error.message))


def reject_entry(
    original_record: dict[str, object] | None,
    error_code: str,
    field_path: str,
    message: str,
    schema_version: str,
    schema_name: str,
    all_errors: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    entry: dict[str, object] = {
        "original_record": original_record,
        "error_code": error_code,
        "field_path": field_path,
        "message": message,
        "schema_name": schema_name,
        "schema_version": schema_version,
        "validation_time": _utc_now(),
    }
    if all_errors:
        entry["all_errors"] = all_errors
    return entry


def _write_jsonl(path: Path, records: Iterable[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            _ = handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            _ = handle.write("\n")


def _write_summary(
    path: Path,
    total_records: int,
    accepted_records: int,
    rejected_records: int,
    errors_by_code: Counter[str],
    schema_name: str,
) -> None:
    _ = path.write_text(
        json.dumps(
            {
                "total_records": total_records,
                "accepted_records": accepted_records,
                "rejected_records": rejected_records,
                "errors_by_code": dict(sorted(errors_by_code.items())),
                "schemas_checked": [f"{schema_name}.schema.json"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _accepted_filename(schema_name: str) -> str:
    if schema_name == "raw_sensor_event":
        return "validated_raw_sensor_events.jsonl"
    return "validated_sensor_events.jsonl"


def _rejected_filename(schema_name: str) -> str:
    if schema_name == "raw_sensor_event":
        return "rejected_raw_sensor_records.jsonl"
    return "rejected_records.jsonl"


def validate_input_dir(
    input_dir: Path,
    output_dir: Path,
    profile_name: str | None = None,
    schema_name: str = "raw_sensor_event",
) -> ValidationResult:
    schema = _load_schema(schema_name)
    schema_version = _schema_version(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    iter_errors = cast(Callable[[dict[str, object]], Iterable[ValidationError]], validator.iter_errors)
    accepted_records: list[dict[str, object]] = []
    rejected_records: list[dict[str, object]] = []
    errors_by_code: Counter[str] = Counter()

    records = _iter_records(input_dir, profile_name, schema_name)
    no_input_records = len(records) == 0
    if no_input_records:
        errors_by_code["no_input_records"] += 1
    for record, parse_error in records:
        if parse_error is not None:
            errors_by_code["invalid_json"] += 1
            rejected_records.append(
                reject_entry(
                    original_record=record,
                    error_code="invalid_json",
                    field_path="/",
                    message=parse_error,
                    schema_version=schema_version,
                    schema_name=schema_name,
                )
            )
            continue

        assert record is not None
        errors = _sort_errors(list(iter_errors(record)))
        if not errors:
            accepted_records.append(record)
            continue

        all_errors = [
            {
                "error_code": _error_code(error),
                "field_path": _field_path(error),
                "message": error.message,
            }
            for error in errors
        ]
        for error in all_errors:
            errors_by_code[error["error_code"]] += 1
        first_error = all_errors[0]
        rejected_records.append(
            reject_entry(
                original_record=record,
                error_code=first_error["error_code"],
                field_path=first_error["field_path"],
                message=first_error["message"],
                schema_version=schema_version,
                schema_name=schema_name,
                all_errors=all_errors,
            )
        )

    accepted_path = output_dir / "generated" / _accepted_filename(schema_name)
    rejected_path = output_dir / "rejected" / _rejected_filename(schema_name)
    summary_path = output_dir / "validation-summary.json"
    _write_jsonl(accepted_path, accepted_records)
    _write_jsonl(rejected_path, rejected_records)
    _write_summary(
        summary_path,
        total_records=len(records),
        accepted_records=len(accepted_records),
        rejected_records=len(rejected_records),
        errors_by_code=errors_by_code,
        schema_name=schema_name,
    )

    return ValidationResult(
        exit_code=1 if rejected_records or no_input_records else 0,
        accepted_path=accepted_path,
        rejected_path=rejected_path,
        summary_path=summary_path,
        total_records=len(records),
        accepted_records=len(accepted_records),
        rejected_records=len(rejected_records),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate generated sensor event JSONL.")
    _ = parser.add_argument("--input", type=Path, required=True)
    _ = parser.add_argument("--output-dir", type=Path, required=True)
    _ = parser.add_argument("--profile", choices=["smoke", "demo"])
    _ = parser.add_argument("--schema", choices=sorted(SCHEMA_PATHS), default="raw_sensor_event")
    return parser


def _parse_args(argv: list[str] | None) -> ValidationCliNamespace:
    namespace = ValidationCliNamespace(input=Path(), output_dir=Path())
    _ = build_parser().parse_args(argv, namespace=namespace)
    return namespace


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = validate_input_dir(args.input, args.output_dir, profile_name=args.profile, schema_name=args.schema)
    message = (
        f"validated {result.total_records} records: "
        + f"{result.accepted_records} accepted, "
        + f"{result.rejected_records} rejected"
    )
    print(message)
    print(f"accepted: {result.accepted_path}")
    print(f"rejected: {result.rejected_path}")
    print(f"summary: {result.summary_path}")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
