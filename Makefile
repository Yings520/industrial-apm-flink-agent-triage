.PHONY: setup generate-demo-data validate-schemas test

PYTHON ?= ./.venv/bin/python
OUTPUT_DIR ?= data
SEED ?= 42
PROFILE ?= demo

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -e .
	$(PYTHON) -m pip install 'pytest>=8.0'

generate-demo-data:
	$(PYTHON) -m src.telemetry_generator.generate_events --profile $(PROFILE) --seed $(SEED) --output-dir $(OUTPUT_DIR)

validate-schemas:
	$(PYTHON) -m src.telemetry_generator.schema_validation --input $(OUTPUT_DIR)/generated --output-dir $(OUTPUT_DIR) --profile $(PROFILE)

test:
	$(PYTHON) -m pytest
