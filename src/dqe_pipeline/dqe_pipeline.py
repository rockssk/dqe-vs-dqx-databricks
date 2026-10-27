"""DQE demo: Lakeflow Spark Declarative Pipelines with Data Quality Expectations.

Pattern shown:
  * bronze  - Auto Loader with schema hints + rescued data column (schema violations never stop the stream)
  * silver  - row-level rules loaded from a metadata file (dqe_rules.json), applied with
              expect_all (warn) / expect_all_or_drop (drop) / expect_all_or_fail (fail)
  * quarantine - the inverse of the drop rules, with the names of the rules each row failed
  * dq_check_* - dataset-level rules (uniqueness, spikes, freshness). Expectations are row-level only,
              so these are expressed as materialized views whose rows are then checked with expectations.
"""
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW = spark.conf.get("raw_path")
RULES = json.load(open(spark.conf.get("rules_path")))

SCHEMA_HINTS = {
    "contacts": "updated_at TIMESTAMP",
    "activities": "sales_activity_date STRING",
    "aum": "aum DOUBLE, cash_flow DOUBLE, month DATE",
    "file_manifest": "landed_at TIMESTAMP",
}


def rules_for(table, action):
    return {r["name"]: r["constraint"] for r in RULES if r["table"] == table and r["action"] == action}


# ── Bronze ──────────────────────────────────────────────────────────────────
def make_bronze(name):
    @dp.table(name=f"bronze_{name}", comment=f"Raw {name} from {RAW}/{name} (Auto Loader, rescued data enabled)")
    def bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", SCHEMA_HINTS[name])
            .option("rescuedDataColumn", "_rescued_data")
            .load(f"{RAW}/{name}")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )


for _name in SCHEMA_HINTS:
    make_bronze(_name)


# ── Silver + quarantine, driven by the rules metadata ───────────────────────
def with_expectations(fn, table):
    for action, decorator in (("warn", dp.expect_all), ("drop", dp.expect_all_or_drop), ("fail", dp.expect_all_or_fail)):
        rules = rules_for(table, action)
        if rules:
            fn = decorator(rules)(fn)
    return fn


def make_silver(table, source_fn):
    def silver():
        return source_fn()

    dp.table(name=f"silver_{table}", comment=f"Validated {table}: drop rules enforced, warn rules tracked in the event log")(
        with_expectations(silver, table)
    )

    drop = rules_for(table, "drop")

    @dp.table(name=f"quarantine_{table}", comment=f"{table} rows that failed at least one drop rule, with the rule names")
    def quarantine():
        failed = F.filter(
            F.array(*[F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(n)) for n, c in drop.items()]),
            lambda x: x.isNotNull(),
        )
        return source_fn().withColumn("_failed_rules", failed).where(F.size("_failed_rules") > 0)


make_silver("contacts", lambda: spark.readStream.table("bronze_contacts"))


def activities_with_contact_check():
    # Expectations can only see the current row, so a cross-table rule (A6) is expressed by joining
    # the reference data into the flow and checking the joined flag.
    known = spark.read.table("bronze_contacts").select("contact_guid").distinct().withColumn("_contact_exists", F.lit(True))
    return (
        spark.readStream.table("bronze_activities")
        .join(known, "contact_guid", "left")
        .withColumn("_contact_exists", F.coalesce("_contact_exists", F.lit(False)))
    )


make_silver("activities", activities_with_contact_check)
make_silver("aum", lambda: spark.readStream.table("bronze_aum"))


# ── Dataset-level rules: workaround pattern (MV + expectation) ─────────────
@dp.materialized_view(name="dq_check_contact_uniqueness", comment="C3: one row per contact_guid")
@dp.expect("C3_contact_guid_unique", "row_count = 1")
def dq_check_contact_uniqueness():
    return spark.read.table("bronze_contacts").groupBy("contact_guid").agg(F.count("*").alias("row_count"))


@dp.materialized_view(name="dq_check_activity_uniqueness", comment="A1: one row per sales_activity_credit_guid")
@dp.expect("A1_credit_guid_unique", "row_count = 1")
def dq_check_activity_uniqueness():
    return spark.read.table("bronze_activities").groupBy("sales_activity_credit_guid").agg(F.count("*").alias("row_count"))


@dp.materialized_view(name="dq_check_aum_uniqueness", comment="F4: one row per advisor and month")
@dp.expect("F4_advisor_month_unique", "advisor_id IS NULL OR row_count = 1")
def dq_check_aum_uniqueness():
    return spark.read.table("bronze_aum").groupBy("advisor_id", "month").agg(F.count("*").alias("row_count"))


@dp.materialized_view(name="dq_check_aum_spike", comment="F3: advisor AUM not more than 3x its trailing 12-month average")
@dp.expect("F3_aum_within_3x_history", "hist_avg IS NULL OR aum <= 3 * hist_avg")
def dq_check_aum_spike():
    return spark.sql("""
        SELECT record_id, advisor_id, month, aum,
               avg(aum) OVER (PARTITION BY advisor_id ORDER BY month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS hist_avg
        FROM bronze_aum
        WHERE advisor_id IS NOT NULL AND aum IS NOT NULL
    """)


@dp.materialized_view(name="dq_check_file_freshness", comment="T1: every source file landed within the 12h SLA")
@dp.expect("T1_file_within_12h", "landed_at >= current_timestamp() - INTERVAL 12 HOURS")
def dq_check_file_freshness():
    return spark.read.table("bronze_file_manifest").select("file_name", "source", "load_type", "landed_at")
