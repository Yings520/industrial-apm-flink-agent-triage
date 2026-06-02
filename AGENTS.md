@/Users/ysc/.codex/RTK.md

# Project Guidance

This repository is a portfolio/current-project implementation of a real-time industrial APM data pipeline with Kafka, Flink event-time processing, data quality controls, and LLM-assisted alert triage.

## GSD Workflow

- Planning artifacts live under `.planning/` and are intentionally local-only for this repo.
- Use `$gsd-discuss-phase 1` to begin execution context for the first phase.
- Use `$gsd-plan-phase <n>` only after phase context is clear.

## Claim Boundary

- Treat Flink and LLM-assisted triage as portfolio/current-project evidence until separate production evidence is supplied.
- Use "LLM-assisted alert triage", "operator support", "root-cause hypotheses", and "evidence-grounded summaries".
- Do not claim autonomous diagnosis, automatic remediation, real factory deployment, production SLA, or real downtime/MTTR reduction.
