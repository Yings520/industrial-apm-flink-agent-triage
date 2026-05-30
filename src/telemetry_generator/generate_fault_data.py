#!/usr/bin/env python3
"""Generate synthetic fault timeline, work orders, and inspection records.

Usage: uv run python -m src.telemetry_generator.generate_fault_data --seed 42 --days 30
"""
from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.telemetry_generator.config import build_asset_id, load_asset_config
from src.telemetry_generator.fault_injector import (
    FaultEvent,
    build_degradation_map,
    generate_fault_timeline,
    write_fault_timeline,
)
from src.telemetry_generator.models import AssetConfig
from src.telemetry_generator.work_order_gen import (
    InspectionRecord,
    WorkOrder,
    generate_inspections,
    generate_work_orders,
    write_inspections,
    write_work_orders,
)


def generate_all(
    *,
    seed: int = 42,
    days: int = 30,
    output_dir: Path = Path("data/generated"),
) -> dict[str, Path]:
    asset_config = load_asset_config()
    start_date = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)

    all_faults: list[FaultEvent] = []
    all_work_orders: list[WorkOrder] = []
    all_inspections: list[InspectionRecord] = []

    for tenant in asset_config["tenants"]:
        tenant_id = tenant["tenant_id"]
        for plant in tenant["plants"]:
            plant_id = plant["plant_id"]
            archetype = plant["archetype"]
            arch_config = asset_config["archetypes"][archetype]
            asset_types = arch_config["asset_types"]

            for asset_index in range(1, asset_config["assets_per_plant"] + 1):
                asset_id = build_asset_id(plant_id, asset_index)
                asset_type = asset_types[(asset_index - 1) % len(asset_types)]

                components = arch_config.get("components", [])
                for comp in components:
                    comp_type = comp["component_type"]
                    fault_count = comp.get("count_per_asset", 1)

                    for fc in range(min(fault_count, 2)):
                        failure_modes = [
                            "fm_bearing_inner_race_wear",
                            "fm_bearing_outer_race_wear",
                            "fm_motor_winding_overheat",
                            "fm_gearbox_tooth_wear",
                            "fm_pump_cavitation",
                            "fm_seal_leakage",
                            "fm_compressor_surge",
                            "fm_turbine_blade_erosion",
                            "fm_chiller_refrigerant_leak",
                        ]
                        fm_id = failure_modes[hash(f"{tenant_id}|{asset_id}|{comp_type}|{fc}") % len(failure_modes)]

                        metrics_map: dict[str, str] = {
                            "bearing": "vibration",
                            "motor": "temperature",
                            "gearbox": "vibration",
                            "pump_impeller": "flow_rate",
                            "seal": "pressure",
                            "compressor_stage": "vibration",
                            "turbine_blade": "vibration",
                            "heat_exchanger_bundle": "temperature",
                            "chiller_compressor": "temperature",
                            "hydraulic_cylinder": "pressure",
                            "valve_actuator": "pressure",
                            "membrane_module": "pressure",
                            "aerator_diffuser": "flow_rate",
                            "boiler_tube": "temperature",
                            "air_handler_fan": "vibration",
                            "cooling_tower_fan": "vibration",
                            "furnace_burner": "temperature",
                            "rolling_mill_roll": "vibration",
                            "caster_mold": "temperature",
                            "conveyor_belt": "vibration",
                            "robot_joint": "temperature",
                            "packaging_sealer": "temperature",
                            "filter_element": "pressure",
                            "fan_impeller": "vibration",
                        }
                        metric = metrics_map.get(comp_type, "vibration")
                        base_values = {
                            "temperature": 68.0 + (asset_index % 7),
                            "vibration": 2.4 + (asset_index % 3),
                            "pressure": 6.0 + (asset_index % 5),
                            "flow_rate": 120.0 + (asset_index * 3),
                            "power_draw": 340.0 + (asset_index * 5),
                        }

                        faults = generate_fault_timeline(
                            tenant_id=tenant_id,
                            plant_id=plant_id,
                            asset_id=asset_id,
                            asset_type=asset_type,
                            component_type=comp_type,
                            failure_mode_id=fm_id,
                            failure_mode_name=fm_id.replace("fm_", "").replace("_", " "),
                            metric_name=metric,
                            base_value=base_values[metric],
                            start_date=start_date,
                            days=days,
                            seed=seed,
                        )
                        all_faults.extend(faults)

                        for fault in faults:
                            wo_list = generate_work_orders(
                                tenant_id=tenant_id,
                                plant_id=plant_id,
                                asset_id=asset_id,
                                asset_type=asset_type,
                                component_type=comp_type,
                                fault=fault,
                                start_date=start_date,
                                days=days,
                                seed=seed,
                            )
                            all_work_orders.extend(wo_list)

                            for wo in wo_list:
                                insp = generate_inspections(wo, comp_type, seed=seed)
                                if insp:
                                    all_inspections.append(insp)

                    # Generate preventive work orders for each component
                    pre_wo = generate_work_orders(
                        tenant_id=tenant_id,
                        plant_id=plant_id,
                        asset_id=asset_id,
                        asset_type=asset_type,
                        component_type=comp_type,
                        fault=None,
                        start_date=start_date,
                        days=days,
                        seed=seed + 1,
                    )
                    all_work_orders.extend(pre_wo)

    output_dir.mkdir(parents=True, exist_ok=True)
    fault_path = write_fault_timeline(all_faults, output_dir / "fault_timeline.jsonl")
    wo_path = write_work_orders(all_work_orders, output_dir / "work_orders.jsonl")
    insp_path = write_inspections(all_inspections, output_dir / "inspections.jsonl")

    return {
        "fault_timeline": fault_path,
        "work_orders": wo_path,
        "inspections": insp_path,
        "fault_count": len(all_faults),
        "work_order_count": len(all_work_orders),
        "inspection_count": len(all_inspections),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic fault timeline and work orders")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()

    result = generate_all(seed=args.seed, days=args.days, output_dir=args.output_dir)
    print(f"Fault timeline: {result['fault_count']} events → {result['fault_timeline']}")
    print(f"Work orders: {result['work_order_count']} records → {result['work_orders']}")
    print(f"Inspections: {result['inspection_count']} records → {result['inspections']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
