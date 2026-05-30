# Design Assumptions

## Explicit decisions that shaped this data model

1. **Industrial archetypes over real factories**: Asset types and failure modes are inspired by real industrial domains (manufacturing, chemical, power, steel, water, facilities) but use synthetic identifiers. No real company names, equipment serial numbers, or factory locations are used.

2. **Rule-first detection, LLM-assisted triage**: Anomalies are detected by deterministic rules (threshold, z-score, drift, heartbeat, flatline). LLMs assist with summarization, hypothesis generation, and recommended checks — they do not perform detection.

3. **SCD Type 2 for dimensional tables**: dim_asset, dim_component, dim_sensor_tag, and dim_threshold_profile use Type 2 slowly-changing dimensions with valid_from/valid_to/is_current. This supports asset replacements, threshold updates, and tag reconfigurations over time without losing historical accuracy.

4. **Star schema over one big table**: Facts (sensor readings, anomalies, quality events, triage evidence) and dimensions (asset, component, tag, metric, threshold profile, failure mode, work order) are separate. This supports scalable queries, consistent definitions, and independent SCD management.

5. **Quality events as first-class facts**: Quality issues (freshness, completeness, range, schema drift, late events, tenant leakage) are modeled as queryable fact rows, not just string flags. This enables quality-aware triage confidence scoring.

6. **Failure modes provide interpretation context, not confirmation**: The FMECA catalog maps component types to known failure modes, PF-interval hints, and recommended checks. An anomaly event may reference a suspected failure_mode_id, but this is a hypothesis, not a confirmed fault.

7. **Work orders as operational evidence, not ground truth**: Work order and inspection records from CMMS/EAM systems are treated as noisy labels. They provide evidence of what was done and found, but the problem_code/cause_code may itself be subject to technician judgment variation.

8. **Multi-tenant from design, single-tenant by default execution**: All contracts and tables include tenant_id. Topic naming is tenant-scoped. The default local execution uses tenant_northwind for simplicity, but the model supports 3+ tenants without schema changes.

9. **synthetic_metadata for generated data boundary**: The `synthetic_metadata` and `scenario` fields are for synthetic/generated data only. Production events would omit these fields. This boundary prevents confusing "scenario=spike" labels with actual anomaly detection outcomes.

10. **SIS and OT boundaries preserved**: This is an APM data model for condition monitoring and operator support. It does not cover safety-instrumented system (SIS) data, safety actions, or OT control authority. Safety system data would require separate contracts, segregation, and certified engineering workflows.

## Out of Scope (explicitly not modeled)

- RUL (Remaining Useful Life) prediction models
- Automated control actions or closed-loop control
- Real-time process control (MES/SCADA integration)
- Safety-instrumented system (SIS) integration
- OT network topology or cybersecurity zoning
- Production scheduling or OEE calculations
- Enterprise Asset Management (EAM) master data integration
- Multi-site global deployment orchestration
