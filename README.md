# Databricks + dbt ELT Platform with Automated Data Quality Gates

A daily revenue data platform for a B2B SaaS company. It takes three siloed, sometimes broken feeds (CRM deals, a billing subscription change feed, product usage metering). Every bad row is quarantined with the reason it failed, and a Great Expectations gate stops the Gold layer from refreshing when something critical is wrong. dbt builds the revenue reporting (ARR, NRR, usage overage, customer health, pipeline) with lineage from every dashboard back to its sources.

**Stack:** Azure Databricks (Lakeflow Declarative Pipelines) · Azure Data Factory · dbt · Delta Lake · Great Expectations · Python

**System design:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has the architecture diagrams and a step-by-step walkthrough of the design, decisions and failure handling.

```
 CRM (CSV) ─────┐                 ┌──────────── Lakeflow / Spark Declarative Pipeline ─────────────┐
 Billing CDC ───┼─► landing zone ─┤ BRONZE streaming tables ─► SILVER (typed, de-duplicated, SCD2)  │
 Usage (JSON) ──┘    (daily)      │                         └► QUARANTINE tables (row + rule broken) │
                                  └───────────────────────────────────┬─────────────────────────────┘
                                         Great Expectations gate ◄────┘   critical failure → Gold NOT refreshed,
                                                    │ pass                  alert sent, dashboards keep last good data
                                                    ▼
                                    dbt GOLD on Delta ─► exposures: 5 BI consumers (Power BI dashboards, reports)
 Orchestration: Azure Data Factory daily (adf/) · locally scripts/orchestrate.py runs the same steps
```

## Results from a local run (`make demo`, 10 simulated business days)

| What | Result |
|---|---|
| Data quality SLA **without** gates (raw drops straight to dashboards) | **50%**: 5 of 10 days shipped data with a critical defect |
| Data quality SLA **with** quarantine + Great Expectations gate | **100%**: 10 of 10 days refreshed Gold with data passing every critical check |
| Bad rows stopped before Gold | 2 negative-MRR changes, 1 unsupported currency, 5 usage rows with no account, 3 future-dated usage rows, 8 duplicate events collapsed |
| Debuggability | Every quarantined row carries the rules it broke and its source file |
| dbt | 8 Gold models and 12 tests pass every day, including ARR reconciliation and retention bounds |
| Lineage | 5 BI consumers traced back to every upstream model and source |
| Revenue KPIs (synthetic) | ARR $8.32M → $9.52M over 10 days; NRR 109.0%, GRR 92.2%; 46 upsell candidates, 4 at-risk customers |

On defect days the gate still passed: quarantine had already removed the bad rows, so Gold refreshed on time with clean numbers. When a problem cannot be fixed by setting rows aside, the same critical checks fail and Gold is not refreshed. The raw-drop runs in the table show exactly which checks would fire.

Full per-day table, quarantine counts, revenue KPIs and lineage: [`docs/RESULTS.md`](docs/RESULTS.md).

## Run it with Airflow (the main way)

All pipeline runs go through Airflow. The DAGs live in [`airflow/dags/`](airflow/dags/):

| DAG | Schedule | Tasks |
|---|---|---|
| `revenue_quality_daily` | `@daily`, 2026-06-01 to 2026-06-10 with catch-up | `prepare` → `land_daily_drop` → `run_lakeflow_pipeline` → `quality_gate` → `dbt_build_gold` → `record_run`, plus `alert_gold_blocked` (runs only if the gate fails) |
| `revenue_quality_report` | manual | `baseline_on_raw_drops` → `dbt_build_scorecard` → `write_report` |
| `revenue_quality_reset` | manual | `reset_local_state` |

Unpausing `revenue_quality_daily` plays the 10 simulated business days in order (about 5 minutes each). If the quality gate fails, `dbt_build_gold` is skipped and `alert_gold_blocked` runs, which is the same branching as the ADF pipeline in [`adf/`](adf/). Then trigger `revenue_quality_report` to measure the SLA and write `docs/RESULTS.md`.

Spark tasks use an Airflow pool, `spark_metastore`, with one slot, because the local embedded metastore allows one Spark process at a time. Create it once:

```bash
airflow pools set spark_metastore 1 "one Spark process at a time"
```

**Start a local Airflow 3 pointed at this repo's DAGs** (web UI on http://localhost:8081, no login):

```bash
uv venv --python 3.12 ~/airflow-venv
curl -sO https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt
uv pip install --python ~/airflow-venv/bin/python -c constraints-3.12.txt apache-airflow==3.2.2 apache-airflow-providers-standard

export AIRFLOW_HOME=~/airflow AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags" AIRFLOW__CORE__LOAD_EXAMPLES=False \
       AIRFLOW__API__PORT=8081 AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True \
       AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8081/execution/ PATH="$HOME/airflow-venv/bin:$PATH"
airflow standalone
```

Tasks call this repo's own `uv` environment (`uv run ...`), so Airflow and the project keep separate dependencies. The constraints file pins Airflow's dependencies to the versions it was tested with; without it a newer Pydantic breaks Airflow 3.2's API server.

## Run it without Airflow (no Databricks or Azure account needed)

Requirements: [uv](https://docs.astral.sh/uv/), Java 17+. No Docker needed.

```bash
make setup     # dependencies + local Spark config (Delta Lake + shared metastore)
make demo      # 10 business days end to end, then the results report (about 40 minutes)
make test      # unit tests (no Spark)
```

| Databricks / Azure | Local stand-in in this repo |
|---|---|
| Lakeflow Declarative Pipelines | Apache Spark 4.2 Declarative Pipelines (`spark-pipelines`), the open-source core of Lakeflow |
| Unity Catalog | Embedded Hive metastore shared by the pipeline, the gate and dbt |
| Delta Lake on ADLS | Delta Lake 4.4 on local disk |
| Databricks job (gate + dbt task) | `quality/run_gate.py` and `dbt build` with the `dbt-spark` session adapter |
| Azure Data Factory pipeline | `scripts/orchestrate.py` runs the same activities in the same order |

The Databricks-native version of the pipeline is [`databricks/lakeflow_pipeline.py`](databricks/lakeflow_pipeline.py), deployed by the bundle in [`databricks/databricks.yml`](databricks/databricks.yml). It uses Auto Loader, built-in expectations and `create_auto_cdc_flow` with SCD Type 2. The ADF pipeline is in [`adf/`](adf/). Those cloud files are written against the documented APIs but have not been deployed from this repo. Everything in the table's right-hand column has been run end to end.

---

## Explained from zero

### The business problem
Revenue reporting depended on separate batch jobs, one per source system, with no shared checks:
- When billing sent a negative MRR or the usage feed sent events twice, those numbers went straight onto leadership dashboards.
- Problems were found when someone noticed a wrong chart.
- Nobody could say which dashboards a broken table affected.

### The pieces

| Piece | Plain-words explanation | What it does here |
|---|---|---|
| **Databricks** | A cloud platform for running Spark and managing data, with notebooks, jobs and a catalog. | Production home of the pipeline, gate and dbt job. |
| **Lakeflow Declarative Pipelines** (formerly Delta Live Tables) | You declare *what* each table should contain; the engine works out the order, runs it incrementally and retries. | Builds Bronze and Silver and the quarantine tables. |
| **Delta Lake** | A table format on top of Parquet files that adds ACID transactions, MERGE, schema enforcement and time travel. | Every table is a Delta table. |
| **Great Expectations** | A library where you write data tests ("this column is never null") and run them as a checkpoint, with an HTML report. | The gate between Silver and Gold. |
| **dbt** | Write SQL `select` statements; dbt builds them in dependency order, tests them and documents lineage. | Gold revenue models, tests, exposures. |
| **Azure Data Factory** | Azure's scheduler and orchestrator, with activities connected by success or failure arrows. | Daily run: copy, then pipeline, then gate, then dbt; alert if the gate fails. |

### One business day, step by step
1. **Land.** The day's files arrive: CRM accounts and deals (CSV), billing subscription changes (JSON change feed) and product usage (JSON).
2. **Bronze.** Streaming tables pick up only the new files (each file is processed exactly once) and store them as received, plus `_ingested_at` and `_source_file`.
3. **Silver with quarantine.**
   - Types are fixed.
   - Duplicate usage events are collapsed to one.
   - Every row is checked against rules such as "account_id present", "mrr ≥ 0", "currency supported" and "usage date not in the future".
   - Rows that pass go to Silver. Rows that fail go to a `quarantine_*` table with the list of rules they broke and the file they came from.
   - Subscription changes become **SCD Type 2 history**: one row per version with `valid_from`, `valid_to` and `is_current`, so MRR can be rebuilt for any past day.
4. **Quality gate.**
   - Great Expectations runs 21 expectations on Silver: uniqueness, nulls, value sets, ranges, referential checks against known accounts, and a volume anomaly check.
   - Each expectation is **critical** or **warning**.
   - Any critical failure stops the run, so Gold is **not** refreshed, ADF sends an alert, and dashboards keep yesterday's correct numbers.
   - Results go to `dq.validation_results` and to Great Expectations Data Docs.
5. **Gold with dbt.**
   - `dim_customer`.
   - `fct_mrr_daily`, rebuilt from the SCD2 history.
   - `rpt_arr_by_product`.
   - `rpt_net_revenue_retention` (NRR and GRR).
   - `rpt_usage_overage`: usage above the plan's included units, priced as overage.
   - `rpt_customer_health`: a 0-100 score from utilisation, MRR trend and open deals.
   - `fct_pipeline`.
   - `rpt_data_quality_daily`.
   - Tests include reconciliations: total ARR equals the sum of customer MRR × 12, and GRR never exceeds 100%.
6. **Lineage.** `dbt/models/exposures.yml` declares the 5 BI consumers. dbt traces each one back through every model to the raw sources (listed in `docs/RESULTS.md`), so before changing a model you know exactly which dashboards it affects.

### How the data quality SLA is measured
**SLA** = the share of daily refreshes where the data reaching dashboards passed every critical check.
- **Without gates (baseline).** The same expectations run on each raw daily drop, after the basic "keep the latest record" de-duplication the old jobs did, but without validation or quarantine. A day with a broken feed fails.
- **With gates.** A day counts if Gold was refreshed with Silver data that passed every critical check. Quarantine removes bad rows, so most bad days still deliver clean, on-time numbers. If something can't be fixed by quarantining rows, the gate blocks instead of publishing wrong numbers.

The simulator injects six kinds of defect at random (null accounts, negative MRR, unsupported currency, duplicate events, future dates, orphan deals). The defect log is in `data/landing/_defect_log.json`.

### Key terms
- **Quarantine**: set bad rows aside with the reason, instead of dropping them silently or failing the whole load.
- **Critical vs warning**: critical checks protect money and keys, so they block. Warnings (such as an unusual row count) are reported but don't block.
- **SCD Type 1 / Type 2**: Type 1 overwrites to keep only the current value. Type 2 keeps every version with validity dates.
- **NRR / GRR**: revenue kept from the customers you started the period with, including upsell (NRR) and excluding it (GRR).
- **Exposure**: dbt's name for something downstream that uses your models, such as a dashboard.

---

## Talking about it in an interview

**30-second version.** "Batch jobs had no lineage and no quality gates, so broken source data reached dashboards and the data-quality SLA was poor. I rebuilt the flow as a declarative pipeline on Delta Lake. Bad rows go to quarantine tables with the rule they broke, and a Great Expectations gate between Silver and Gold blocks the refresh on any critical failure, so dashboards keep the last good numbers instead of wrong ones. dbt builds ARR, NRR, usage overage and customer health, and its exposures give lineage from every dashboard back to source. The SLA went up sharply, and debugging got much faster because every bad row says why it failed and where it came from."

**Likely follow-up questions**
- *Why quarantine instead of failing the load?* Good data keeps flowing and bad rows are kept for investigation. The gate still stops the run when the problem is systemic.
- *Why both pipeline rules and Great Expectations?* Pipeline rules handle row-level problems as data flows. Great Expectations checks table-level properties (uniqueness across the table, referential integrity, volume) and produces an auditable report before anything is published.
- *How do you choose critical vs warning?* Anything that changes money or breaks a join key is critical. Anything that is unusual but plausible is a warning, reviewed the same day.
- *How did you rebuild MRR for past days?* From SCD2 history: for each day, pick the subscription version valid at the end of that day.
- *What does Databricks add over the local version?* Auto Loader with schema evolution, expectations with metrics in the event log, `create_auto_cdc_flow` with SCD2, Unity Catalog lineage and serverless compute.
- *How would ADF know the gate failed?* The gate exits non-zero, so its activity fails. The dbt activity depends on "Succeeded", and a Teams alert depends on "Failed".

## Repo layout
```
generator/generate_daily_drops.py      daily drops from 3 systems + random defects
pipelines/transformations/01_bronze.py Bronze streaming tables
pipelines/transformations/02_silver.py Silver, quarantine, SCD2 history
quality/expectations.py                Great Expectations suites (critical / warning)
quality/run_gate.py                    the Silver -> Gold gate (also scores the raw baseline)
dbt/                                   Gold models, tests, exposures (dbt-spark locally, dbt-databricks in prod)
databricks/                            Lakeflow pipeline + Databricks Asset Bundle
adf/                                   Azure Data Factory pipeline + daily trigger
scripts/orchestrate.py                 local stand-in for the ADF pipeline
scripts/report.py                      writes docs/RESULTS.md
tests/                                 unit tests
```
