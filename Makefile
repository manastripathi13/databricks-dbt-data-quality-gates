.PHONY: setup demo pipeline gate gold test report reset
setup:     ## install deps and write local Spark config
	uv sync && uv run python scripts/render_spark_conf.py
demo:      ## 10 business days: land -> pipeline -> quality gate -> dbt Gold, then the report
	./scripts/run_demo.sh 10
pipeline:  ## run the declarative pipeline once
	./scripts/run_pipeline.sh
gate:      ## quality gate for a date, e.g. make gate DATE=2026-06-05
	SPARK_CONF_DIR=$$PWD/conf uv run python quality/run_gate.py --run-date $(DATE)
gold:      ## dbt build of the Gold layer
	cd dbt && SPARK_CONF_DIR=$$PWD/../conf uv run dbt build --profiles-dir .
report:
	uv run python scripts/report.py
test:      ## unit tests (no Spark needed)
	uv run pytest -q tests
reset:
	./scripts/reset.sh
