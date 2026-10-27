# DQE vs DQX: data quality on Databricks, side by side

Runnable comparison of the two main ways to enforce data quality on Databricks, using the same synthetic data, the same rules, and planted defects as ground truth:

- **DQE**: Data Quality **Expectations** in Lakeflow Spark Declarative Pipelines (SDP, formerly DLT)
- **DQX**: [Databricks Labs DQX](https://github.com/databrickslabs/dqx), a PySpark data quality framework, used standalone and **inside SDP**
- **DQM**: Unity Catalog Data Quality Monitoring, covered in the FAQ for freshness and volume

The FAQ, [`docs/FAQ.md`](docs/FAQ.md), covers capabilities, quarantine, streaming, rule storage, support and a recommendation. It also has a **Mermaid decision tree** for choosing DQE, DQX or DQM rule by rule.

## Results

| | DQE | DQX |
|---|---|---|
| Defects caught (627 planted, 17 rule types) | 627 / 627 | 627 / 627 |
| False positives | 0 | 0 |
| Rule types where the failing rows are identified | 11 / 17 (warn and dataset-level rules report counts only) | 17 / 17 |
| Extra objects needed for dataset-level rules | 5 MVs + 1 join | none |

Run with the DQX-inside-SDP pipeline, DQX produced exactly the same silver and quarantine rows as standalone DQX.

## What's in the bundle

One job, `dq-demo (DQE vs DQX)`, in a Declarative Automation Bundle:

| Task | What it does | Output tables |
|---|---|---|
| `generate_data` | Synthetic contacts, activities, monthly AUM and a file manifest written as JSON to a UC volume, with 627 planted defects | `planted_defects` (ground truth) |
| `dqe_pipeline` | SDP pipeline `dqe-expectations-demo`. Bronze: Auto Loader with rescued data. Silver: expectations loaded from `src/rules/dqe_rules.json` (warn / drop / fail). Quarantine tables record the names of the rules each row failed. Dataset-level rules run as MVs with expectations. The event log is published to UC | `bronze_*`, `silver_*`, `quarantine_*`, `dq_check_*`, `dqe_event_log` |
| `dqx_checks` | Profiles a table and generates rule candidates. Saves the YAML checks in `src/rules/dqx/` to a Delta checks table and loads them back. Applies row- and dataset-level checks (`is_unique`, `foreign_key`, `sql_query` spike, `is_data_fresh`) and splits the output into silver and quarantine | `dqx_generated_candidates`, `dqx_checks`, `dqx_annotated_*`, `dqx_silver_*`, `dqx_quarantine_*`, `dqx_results_summary` |
| `dqx_in_sdp` | SDP pipeline `dqx-in-sdp-demo` (`src/dqx_in_sdp/dqx_sdp_pipeline.py`). Loads the same rules from `dqx_checks` and runs the row-level DQX checks on the stream. Silver has one DQE expectation over DQX's verdict, so the counts land in the event log. Quarantine keeps per-row reasons. Dataset-level checks run in MVs | `dqx_sdp_silver_*`, `dqx_sdp_quarantine_*`, `dqx_sdp_dataset_*`, `dqx_sdp_event_log` |
| `assessment` | Scores each framework against the ground truth, reads expectation metrics from the event log, and maps 18 common rule patterns to DQE / DQX / DQM | `dq_assessment_scorecard`, `dqe_expectation_results`, `rule_pattern_mapping` |

Rule IDs are shared between the frameworks (for example `C1_contact_has_no_crd` in both), so results compare 1:1.

## Deploy and run

Requirements: a Unity Catalog workspace with serverless compute, and a recent Databricks CLI.

```bash
databricks bundle deploy --var catalog=<your_catalog> --var schema=dq_demo
databricks bundle run dq_demo --var catalog=<your_catalog> --var schema=dq_demo
```
Set your workspace with `DATABRICKS_HOST`/profile, or add a `workspace:` block under `targets.dev` in `databricks.yml`.

The dashboard is optional. `dashboards/dq_assessment.lvdash.json` queries `main.dq_demo`; replace that with your catalog and schema, then import the file in the workspace UI (Dashboards → Import) or create it with `databricks lakeview create`.

## Word version of the FAQ

```bash
python3 scripts/build_docx.py docs/FAQ.md FAQ.docx docs/decision_tree_ascii.txt
```
Standard library only. Word can't render Mermaid, so the decision tree is replaced by the ASCII version in `docs/decision_tree_ascii.txt`.

## Not covered here
DQX Studio, AI-assisted rule generation, DQM anomaly detection and SQL alerts are described in the FAQ but not built in this demo.

All data is synthetic.
