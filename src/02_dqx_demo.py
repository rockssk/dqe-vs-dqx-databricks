# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · DQX demo — same data, same rules, plus dataset-level checks
# MAGIC
# MAGIC 1. **Profile** a table and let DQX propose rule candidates (cold start)
# MAGIC 2. **Store** curated YAML rules in a Delta checks table (metadata-driven, versionable)
# MAGIC 3. **Apply** row- *and* dataset-level checks (uniqueness, foreign key, spike vs. history, freshness)
# MAGIC 4. **Split** into silver vs. quarantine — every bad row carries *which* rule failed and *why*
# MAGIC 5. **Summarize** results into a table the dashboard reads

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dq_demo")
dbutils.widgets.text("rules_dir", "")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RULES_DIR = dbutils.widgets.get("rules_dir")
FQ = f"{CATALOG}.{SCHEMA}"
CHECKS_TABLE = f"{FQ}.dqx_checks"
TABLES = ["contacts", "activities", "aum", "file_manifest"]

# COMMAND ----------

import time

import yaml
from importlib.metadata import version
from databricks.labs.dqx.config import TableChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

ws = WorkspaceClient()
dq_engine = DQEngine(ws)
print("DQX version:", version("databricks-labs-dqx"))

# COMMAND ----------

# MAGIC %md ## 1 · Profile and generate rule candidates (cold start)

# COMMAND ----------

contacts_bronze = spark.table(f"{FQ}.bronze_contacts")
summary_stats, profiles = DQProfiler(ws).profile(contacts_bronze)
generated = DQGenerator(ws).generate_dq_rules(profiles)
print(f"Profiler proposed {len(generated)} candidate checks for contacts")
print(yaml.safe_dump(generated[:8], sort_keys=False))

(spark.createDataFrame([(c.get("name") or c["check"]["function"], c["criticality"], yaml.safe_dump(c, sort_keys=False)) for c in generated],
                       "check_name string, criticality string, check_yaml string")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dqx_generated_candidates"))

# COMMAND ----------

# MAGIC %md ## 2 · Store curated rules in a Delta checks table

# COMMAND ----------

for t in TABLES:
    with open(f"{RULES_DIR}/{t}.yml") as fh:
        # Rules reference tables as {{source_schema}}.<table>, resolved here so the stored rules are fully qualified
        checks = yaml.safe_load(fh.read().replace("{{source_schema}}", FQ))
    assert not DQEngine.validate_checks(checks).has_errors, f"invalid checks for {t}"
    dq_engine.save_checks(checks, TableChecksStorageConfig(location=CHECKS_TABLE, run_config_name=t, mode="overwrite"))

display(spark.table(CHECKS_TABLE))

# COMMAND ----------

# MAGIC %md ## 3–4 · Apply checks (loaded back from the table) and split silver / quarantine

# COMMAND ----------

timings = []

for t in TABLES:
    checks = dq_engine.load_checks(TableChecksStorageConfig(location=CHECKS_TABLE, run_config_name=t))
    df = spark.table(f"{FQ}.bronze_{t}")

    start = time.time()
    annotated = dq_engine.apply_checks_by_metadata(df, checks)
    annotated.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dqx_annotated_{t}")
    timings.append((t, df.count(), len(checks), round(time.time() - start, 1)))

    annotated = spark.table(f"{FQ}.dqx_annotated_{t}")
    (annotated.where("_errors IS NULL").drop("_errors", "_warnings")
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dqx_silver_{t}"))
    (annotated.where("_errors IS NOT NULL")
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dqx_quarantine_{t}"))

spark.createDataFrame(timings, "table_name string, input_rows long, checks long, seconds double") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dqx_run_timings")
display(spark.table(f"{FQ}.dqx_run_timings"))

# COMMAND ----------

# MAGIC %md
# MAGIC The one-call split API does the same thing (shown here for contacts):
# MAGIC ```python
# MAGIC valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(df, checks)
# MAGIC ```

# COMMAND ----------

# MAGIC %md ### What a quarantined row looks like — rule, message, and columns, per row

# COMMAND ----------

display(spark.table(f"{FQ}.dqx_quarantine_activities")
        .select("record_id", "contact_guid", "sales_activity_date", "_errors", "_warnings").limit(20))

# COMMAND ----------

# MAGIC %md ## 5 · Results summary (feeds the dashboard)

# COMMAND ----------

results = None
for t in TABLES:
    a = spark.table(f"{FQ}.dqx_annotated_{t}")
    total = a.count()
    for col, crit in (("_errors", "error"), ("_warnings", "warn")):
        part = (a.where(F.col(col).isNotNull())
                .select(F.explode(col).alias("r"))
                .groupBy(F.col("r.name").alias("check_name"), F.col("r.message").alias("sample_message"))
                .count()
                .groupBy("check_name").agg(F.sum("count").alias("failed_rows"), F.first("sample_message").alias("sample_message"))
                .withColumn("table_name", F.lit(t)).withColumn("criticality", F.lit(crit))
                .withColumn("total_rows", F.lit(total)))
        results = part if results is None else results.unionByName(part)

(results.withColumn("rule_id", F.split("check_name", "_")[0])
 .withColumn("failed_pct", F.round(100 * F.col("failed_rows") / F.col("total_rows"), 3))
 .withColumn("run_ts", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dqx_results_summary"))

display(spark.table(f"{FQ}.dqx_results_summary").orderBy("table_name", "rule_id"))
