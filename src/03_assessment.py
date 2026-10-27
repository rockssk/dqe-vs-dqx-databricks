# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Assessment — DQE vs DQX on the same data and rules
# MAGIC
# MAGIC * **Detection scorecard**: planted defects (ground truth) vs. what each framework flagged
# MAGIC * **Observability**: does the framework tell you *which rows* failed, or only *how many*?
# MAGIC * **Rule-pattern coverage**: common data quality rule patterns mapped to native / workaround / not supported

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dq_demo")
FQ = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

from pyspark.sql import functions as F

# Columns that identify a defect, per rule (must match planted_defects.match_col)
MATCH_EXPR = {
    "record_id": "record_id",
    "contact_guid": "contact_guid",
    "sales_activity_credit_guid": "sales_activity_credit_guid",
    "advisor_month": "concat(advisor_id, '|', cast(month AS string))",
    "file_name": "file_name",
}
planted = spark.table(f"{FQ}.planted_defects")
match_col = {r.rule_id: r.match_col for r in planted.select("rule_id", "match_col").distinct().collect()}


def flagged_from(table, rules_col_expr, table_name):
    """(rule_id, match_value) for each rule a row failed, using the rule's own match column."""
    df = spark.table(table).withColumn("_rule", F.explode(F.expr(rules_col_expr))).withColumn("rule_id", F.split("_rule", "_")[0])
    cols = spark.table(table).columns
    parts = []
    for rid, mc in match_col.items():
        needed = ["advisor_id", "month"] if mc == "advisor_month" else [mc]
        if all(c in cols for c in needed):
            parts.append(df.where(F.col("rule_id") == rid).select("rule_id", F.expr(MATCH_EXPR[mc]).alias("match_value")))
    out = None
    for p in parts:
        out = p if out is None else out.unionByName(p)
    return out.withColumn("table_name", F.lit(table_name)) if out is not None else None


# COMMAND ----------

# MAGIC %md ## DQX — every flagged row carries its failing rule names

# COMMAND ----------

dqx_flags = None
for t in ["contacts", "activities", "aum", "file_manifest"]:
    expr = "concat(coalesce(transform(_errors, x -> x.name), array()), coalesce(transform(_warnings, x -> x.name), array()))"
    f = flagged_from(f"{FQ}.dqx_annotated_{t}", expr, t)
    if f is not None:
        dqx_flags = f if dqx_flags is None else dqx_flags.unionByName(f)
dqx_flags = dqx_flags.distinct()

# COMMAND ----------

# MAGIC %md ## DQE — row identity only where a quarantine table exists; everything else is a count in the event log

# COMMAND ----------

dqe_flags = None
for t in ["contacts", "activities", "aum"]:
    f = flagged_from(f"{FQ}.quarantine_{t}", "_failed_rules", t)
    if f is not None:
        dqe_flags = f if dqe_flags is None else dqe_flags.unionByName(f)
dqe_flags = dqe_flags.distinct()

dqe_counts = spark.sql(f"""
  WITH latest AS (
    SELECT origin.update_id AS update_id FROM {FQ}.dqe_event_log
    WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1)
  SELECT e.name AS expectation, e.dataset, sum(e.passed_records) AS passed_records, sum(e.failed_records) AS failed_records
  FROM {FQ}.dqe_event_log
  LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) x AS e
  WHERE event_type = 'flow_progress' AND origin.update_id = (SELECT update_id FROM latest)
  GROUP BY ALL
""").withColumn("rule_id", F.split("expectation", "_")[0])
dqe_counts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dqe_expectation_results")
display(dqe_counts.orderBy("dataset", "expectation"))

# COMMAND ----------

# MAGIC %md ## Detection scorecard

# COMMAND ----------

truth = planted.select("rule_id", "table_name", "defect", "match_value").distinct()


def score(flags, prefix):
    tp = truth.join(flags.select("rule_id", "match_value"), ["rule_id", "match_value"], "inner") \
        .groupBy("rule_id").agg(F.count("*").alias(f"{prefix}_detected"))
    flagged = flags.groupBy("rule_id").agg(F.countDistinct("match_value").alias(f"{prefix}_flagged"))
    return tp.join(flagged, "rule_id", "full")


dqe_mechanism = spark.createDataFrame([
    ("C1", "row: drop+quarantine"), ("C2", "row: drop+quarantine"), ("C4", "row: drop+quarantine"),
    ("C5", "row: drop+quarantine"), ("C6", "row: drop+quarantine"), ("A2", "row: drop+quarantine"),
    ("A3", "row: drop+quarantine"), ("A4", "row: drop+quarantine"), ("A6", "row: join + drop+quarantine"),
    ("F1", "row: rescued data + drop"), ("F2", "row: drop+quarantine"), ("A5", "row: warn (count only)"),
    ("C3", "dataset: MV workaround (count only)"), ("A1", "dataset: MV workaround (count only)"),
    ("F4", "dataset: MV workaround (count only)"), ("F3", "dataset: MV + window workaround (count only)"),
    ("T1", "dataset: MV workaround (count only)")], "rule_id string, dqe_mechanism string")

scorecard = (
    truth.groupBy("rule_id", "table_name", "defect").agg(F.count("*").alias("planted"))
    .join(score(dqe_flags, "dqe"), "rule_id", "left")
    .join(dqe_counts.groupBy("rule_id").agg(F.sum("failed_records").alias("dqe_event_log_failed")), "rule_id", "left")
    .join(score(dqx_flags, "dqx"), "rule_id", "left")
    .join(dqe_mechanism, "rule_id", "left")
    .withColumn("dqe_row_identity", F.col("dqe_detected").isNotNull())
    .withColumn("dqx_row_identity", F.lit(True))
    .fillna(0, ["dqe_detected", "dqe_flagged", "dqe_event_log_failed", "dqx_detected", "dqx_flagged"])
    .withColumn("dqe_caught", F.when(F.col("dqe_row_identity"), F.col("dqe_detected")).otherwise(F.col("dqe_event_log_failed")))
    .withColumn("dqe_recall_pct", F.round(100 * F.least(F.col("dqe_caught"), F.col("planted")) / F.col("planted"), 1))
    .withColumn("dqx_recall_pct", F.round(100 * F.col("dqx_detected") / F.col("planted"), 1))
    .withColumn("dqx_false_positives", F.col("dqx_flagged") - F.col("dqx_detected"))
    .orderBy("table_name", "rule_id")
)
scorecard.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dq_assessment_scorecard")
display(spark.table(f"{FQ}.dq_assessment_scorecard"))

# COMMAND ----------

# MAGIC %md ## Common rule patterns — coverage by framework

# COMMAND ----------

N, W = "Native", "Workaround"
mapping = [
    # rule pattern, dimension, DQE, DQX, DQX check, recommended, note
    ("Required field (not null / not empty)", "Completeness", N, N, "is_not_null / is_not_null_and_not_empty", "DQE", "Pure row rule"),
    ("Conditional required field (IF a THEN b NOT NULL)", "Completeness", N, N, "sql_expression", "DQE", "Pure row rule"),
    ("Format / regex (GUID, email, codes)", "Validity", N, N, "regex_match", "DQE", "Pure row rule"),
    ("Date / timestamp format", "Validity", N, N, "is_valid_date / is_valid_timestamp", "DQE", "Pure row rule"),
    ("Allowed values / numeric range", "Validity", N, N, "is_in_list / is_in_range", "DQE", "Pure row rule"),
    ("Flag consistent with other columns", "Accuracy", N, N, "sql_expression", "DQE", "Pure row rule"),
    ("Values match the expected schema", "Consistency", N, N, "has_valid_schema", "DQE", "Auto Loader schema hints + rescued data"),
    ("Uniqueness / primary key", "Uniqueness", W, N, "is_unique", "DQX", "DQE needs a group-by MV"),
    ("Composite-key duplicates", "Uniqueness", W, N, "is_unique (composite)", "DQX", "DQE needs a group-by MV"),
    ("Foreign key / lookup exists", "Consistency", W, N, "foreign_key", "DQX", "DQE possible if the lookup is joined in-flow"),
    ("Layer rollup completeness (raw to silver to gold)", "Consistency", W, N, "foreign_key / compare_datasets", "DQX", "Dataset-level"),
    ("Cross-system match (CRM vs. vendor feed)", "Accuracy", W, N, "compare_datasets", "DQX", "Cross-system comparison"),
    ("Row count matches source manifest", "Consistency", W, N, "is_aggr_equal / sql_query", "DQX", "Dataset-level"),
    ("Aggregate spike vs. historical average", "Accuracy", W, N, "sql_query / has_no_aggr_outliers", "DQX", "Demonstrated as F3"),
    ("Distinct-count drift per period", "Accuracy", W, N, "sql_query / has_no_aggr_outliers", "DQX", "Dataset-level"),
    ("New / unknown reference values", "Accuracy", W, N, "foreign_key (vs. master)", "DQX", "Dataset-level"),
    ("Source file freshness SLA", "Timeliness", W, N, "is_data_fresh", "DQM + DQX", "Demonstrated as T1"),
    ("Table freshness / volume anomaly", "Timeliness", W, N, "is_data_fresh_per_time_window", "DQM", "DQM learns the pattern; no rule to write"),
]
cols = "rule_pattern string, dimension string, dqe_support string, dqx_support string, dqx_check string, recommended string, note string"
spark.createDataFrame(mapping, cols).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.rule_pattern_mapping")

display(spark.sql(f"""
  SELECT recommended, count(*) AS patterns,
         sum(CASE WHEN dqe_support = 'Native' THEN 1 ELSE 0 END) AS dqe_native,
         sum(CASE WHEN dqx_support = 'Native' THEN 1 ELSE 0 END) AS dqx_native
  FROM {FQ}.rule_pattern_mapping GROUP BY recommended ORDER BY patterns DESC"""))
