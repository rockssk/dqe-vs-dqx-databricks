"""DQX inside Lakeflow Spark Declarative Pipelines.

Same rules as the batch DQX job, loaded from the shared Delta checks table (dqx_checks), applied in SDP:

  dqx_sdp_checked_<t>     private streaming table   stream from bronze, DQX row-level checks add _errors / _warnings.
                                                    Persisted once, so silver and quarantine don't re-run the checks.
  dqx_sdp_silver_<t>      streaming table  one DQE expectation, "no DQX errors" (drop), so DQX's verdict
                                           also shows up as pass/fail counts in the event log and pipeline UI
  dqx_sdp_quarantine_<t>  streaming table  rows with _errors, carrying DQX's per-row rule name and message
  dqx_sdp_dataset_<t>     materialized view  dataset-level checks (is_unique, foreign_key, sql_query, ...) need
                                           the whole table, not one micro-batch, so they run on a batch read;
                                           holds only the rows that failed, with their reasons

Rules are split by DQX's own rule type (DQRowRule vs DQDatasetRule), not by a hand-kept function list.
Foreign-key rules name their reference with ref_table, so no ref_dfs wiring is needed here.
"""
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.config import TableChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE = spark.conf.get("source_schema")      # catalog.schema holding bronze_* (written by the DQE pipeline)
CHECKS_TABLE = spark.conf.get("checks_table")  # Delta checks table written by the DQX job
TABLES = ["contacts", "activities", "aum"]

dq_engine = DQEngine(WorkspaceClient())


def split_checks(table):
    stored = dq_engine.load_checks(TableChecksStorageConfig(location=CHECKS_TABLE, run_config_name=table))
    rules = deserialize_checks(stored)
    row = [r for r in rules if isinstance(r, DQRowRule)]
    dataset = [r for r in rules if isinstance(r, DQDatasetRule)]
    unknown = [r for r in rules if not isinstance(r, (DQRowRule, DQDatasetRule))]
    if unknown:
        raise ValueError(f"{table}: unsupported rule types {[type(r).__name__ for r in unknown]}")
    return row, dataset


def make_flows(table):
    row_checks, dataset_checks = split_checks(table)

    @dp.table(name=f"dqx_sdp_checked_{table}", private=True, comment=f"{table} with DQX row-level results (pipeline-internal)")
    def checked():
        return dq_engine.apply_checks(spark.readStream.table(f"{SOURCE}.bronze_{table}"), row_checks)

    @dp.table(name=f"dqx_sdp_silver_{table}", comment=f"{table} rows with no DQX row-level errors (warnings kept)")
    @dp.expect_or_drop("dqx_no_errors", "_dqx_error_count = 0")
    def silver():
        return (spark.readStream.table(f"dqx_sdp_checked_{table}")
                .withColumn("_dqx_error_count", F.when(F.col("_errors").isNull(), 0).otherwise(F.size("_errors")))
                .drop("_errors"))

    @dp.table(name=f"dqx_sdp_quarantine_{table}", comment=f"{table} rows that failed a DQX row-level check, with reasons")
    def quarantine():
        return spark.readStream.table(f"dqx_sdp_checked_{table}").where("_errors IS NOT NULL")

    if dataset_checks:
        # Recomputed in full on every update: DQX puts a per-run id in the plan, so the MV can't refresh incrementally.
        @dp.materialized_view(name=f"dqx_sdp_dataset_{table}", comment=f"{table} rows failing DQX dataset-level checks")
        def dataset():
            annotated = dq_engine.apply_checks(spark.read.table(f"{SOURCE}.bronze_{table}"), dataset_checks)
            return annotated.where("_errors IS NOT NULL OR _warnings IS NOT NULL")


for _t in TABLES:
    make_flows(_t)
