# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generate synthetic wealth-management-style data with planted defects
# MAGIC
# MAGIC Writes raw JSON files for **contacts**, **activities**, **aum** (monthly AUM / cash flow) and a **file manifest**
# MAGIC to a UC volume. Every defect is planted on purpose and recorded in `planted_defects` (ground truth), so the
# MAGIC assessment notebook can measure exactly what DQE (SDP expectations) and DQX each catch.
# MAGIC
# MAGIC Defects cover common wealth-management rules: GUID format, contacts with a firm CRD, duplicates, missing FKs,
# MAGIC bad dates, stale files, AUM spikes vs. historical average, schema violations.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dq_demo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# COMMAND ----------

import datetime as dt
import json
import os
import random
import shutil
import uuid

rng = random.Random(42)
NOW = dt.datetime.utcnow().replace(microsecond=0)


def guid():
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def pick(population, k, exclude=()):
    """Pick k distinct items not in exclude, so each planted defect lands on its own rows."""
    pool = [p for p in population if p not in exclude]
    return set(rng.sample(pool, k))


planted = []  # (rule_id, table, match_col, match_value, defect)

# COMMAND ----------

# MAGIC %md ## Contacts (CRM / marketing / vendor feed)

# COMMAND ----------

FIRST = ["ava", "ben", "chloe", "dev", "ella", "finn", "grace", "hari", "isla", "jon", "kai", "lena", "mo", "nora", "omar", "priya"]
LAST = ["adams", "baker", "chen", "diaz", "evans", "foster", "gupta", "hill", "iyer", "jones", "kim", "lopez", "moore", "nash"]
DOMAINS = ["advisorco.com", "wealthpartners.com", "finplan.net", "rtadvisors.com"]
N_CONTACTS = 5000

contacts = []
for i in range(N_CONTACTS):
    f, l = rng.choice(FIRST), rng.choice(LAST)
    src = rng.choices(["CRM", "MARKETING", "VENDOR"], weights=[60, 25, 15])[0]
    contacts.append({
        "record_id": f"C-{i:06d}",
        "contact_guid": guid(),
        "source": src,
        "first_name": f.title(),
        "last_name": l.title(),
        "email": f"{f}.{l}{i}@{rng.choice(DOMAINS)}",
        "crd_number": None,  # CRD applies to companies only; contacts carry advisor_crd
        "advisor_crd": str(rng.randint(1000000, 9999999)),
        "company_guid": guid(),
        "third_party_identifier_guid": guid(),
        "office_id": f"OFF{rng.randint(1, 900):05d}",
        "updated_at": (NOW - dt.timedelta(minutes=rng.randint(10, 600))).isoformat(),
    })

idx = list(range(N_CONTACTS))
used = set()
vendor = [i for i in idx if contacts[i]["source"] == "VENDOR"]

c1 = pick(idx, 40, used); used |= c1
for i in c1:
    contacts[i]["crd_number"] = str(rng.randint(100000, 999999))
    planted.append(("C1", "contacts", "record_id", contacts[i]["record_id"], "Contact has a CRD number"))

c2 = pick(idx, 60, used); used |= c2
for i in c2:
    contacts[i]["email"] = rng.choice(["{f}.{l}@", "{f}{l}.advisorco.com", "{f} {l}@advisorco.com", "{f}@@finplan"]).format(
        f=contacts[i]["first_name"].lower(), l=contacts[i]["last_name"].lower())
    planted.append(("C2", "contacts", "record_id", contacts[i]["record_id"], "Invalid email format"))

c4 = pick(vendor, 30, used); used |= c4
for i in c4:
    contacts[i]["advisor_crd"] = None
    planted.append(("C4", "contacts", "record_id", contacts[i]["record_id"], "Vendor contact missing advisor CRD"))

c5 = pick(idx, 35, used); used |= c5
for i in c5:
    contacts[i]["company_guid"] = None
    planted.append(("C5", "contacts", "record_id", contacts[i]["record_id"], "Missing company_guid"))

c6 = pick(idx, 45, used); used |= c6
for i in c6:
    g = contacts[i]["third_party_identifier_guid"]
    contacts[i]["third_party_identifier_guid"] = rng.choice([g.replace("-", ""), g.upper()[:-4], "{" + g + "}", g[:8]])
    planted.append(("C6", "contacts", "record_id", contacts[i]["record_id"], "Malformed third-party identifier GUID"))

# C3: duplicate marketing contacts (clean copies of existing rows under a new record_id)
c3 = pick([i for i in idx if contacts[i]["source"] == "MARKETING"], 25, used); used |= c3
for n, i in enumerate(sorted(c3)):
    dup = dict(contacts[i], record_id=f"C-D{n:05d}")
    contacts.append(dup)
    planted.append(("C3", "contacts", "contact_guid", dup["contact_guid"], "Duplicate marketing contact"))

valid_contact_guids = [c["contact_guid"] for c in contacts[:N_CONTACTS]]

# COMMAND ----------

# MAGIC %md ## Sales activities

# COMMAND ----------

N_ACT = 20000
TYPES = ["appointment", "phone_call", "email", "webinar"]
activities = []
for i in range(N_ACT):
    t = rng.choices(TYPES, weights=[30, 35, 25, 10])[0]
    ts = NOW - dt.timedelta(days=rng.randint(0, 290), minutes=rng.randint(0, 1440))
    activities.append({
        "record_id": f"A-{i:06d}",
        "sales_activity_credit_guid": guid(),
        "sales_activity_detail_guid": guid(),
        "sales_activity_party_guid": guid(),
        "contact_guid": rng.choice(valid_contact_guids),
        "activity_type": t,
        "mtg_flag": "Y" if t == "appointment" else "N",
        "credited_flag": rng.choice(["Y", "N"]),
        "sales_activity_date": ts.strftime("%Y-%m-%d %H:%M:%S"),
    })

idx = list(range(N_ACT))
used = set()
appts = [i for i in idx if activities[i]["activity_type"] == "appointment"]

a2 = pick(idx, 70, used); used |= a2
for i in a2:
    activities[i]["sales_activity_detail_guid"] = None
    planted.append(("A2", "activities", "record_id", activities[i]["record_id"], "Missing sales_activity_detail_guid"))

a3 = pick(idx, 40, used); used |= a3
for i in a3:
    activities[i]["sales_activity_party_guid"] = None
    planted.append(("A3", "activities", "record_id", activities[i]["record_id"], "Missing sales_activity_party_guid"))

a4 = pick(idx, 55, used); used |= a4
for i in a4:
    d = activities[i]["sales_activity_date"]
    activities[i]["sales_activity_date"] = rng.choice([d.replace("-", "/"), d[:10], "N/A", "2026-13-45 25:61:00", d[8:10] + "-" + d[5:7] + "-" + d[:4]])
    planted.append(("A4", "activities", "record_id", activities[i]["record_id"], "Invalid activity date format"))

a5 = pick(appts, 65, used); used |= a5
for i in a5:
    activities[i]["mtg_flag"] = "N"
    planted.append(("A5", "activities", "record_id", activities[i]["record_id"], "Appointment not flagged as meeting"))

a6 = pick(idx, 45, used); used |= a6
for i in a6:
    activities[i]["contact_guid"] = guid()  # contact that does not exist
    planted.append(("A6", "activities", "record_id", activities[i]["record_id"], "Activity references unknown contact"))

a1 = pick(idx, 50, used); used |= a1
for n, i in enumerate(sorted(a1)):
    dup = dict(activities[i], record_id=f"A-D{n:05d}")
    activities.append(dup)
    planted.append(("A1", "activities", "sales_activity_credit_guid", dup["sales_activity_credit_guid"], "Duplicate credited activity GUID"))

# COMMAND ----------

# MAGIC %md ## Monthly AUM / cash flow (13 months per advisor)

# COMMAND ----------

CATEGORIES = ["Large Blend", "Large Growth", "Large Value", "Mid-Cap Blend", "Small Blend", "Foreign Large Blend",
              "Intermediate Core Bond", "Short-Term Bond", "Muni National Interm", "Target-Date 2040", "Allocation 50-70%", "Money Market"]
months = []
m = dt.date(NOW.year, NOW.month, 1)
for _ in range(13):
    months.append(m)
    m = (m - dt.timedelta(days=1)).replace(day=1)
months.reverse()
CUR = months[-1]

aum = []
n = 0
for a in range(800):
    adv = f"ADV{a:05d}"
    office = f"OFF{rng.randint(1, 900):05d}"
    cat = rng.choice(CATEGORIES)
    base = rng.uniform(2e6, 400e6)
    for mo in months:
        base *= rng.uniform(0.97, 1.04)
        aum.append({
            "record_id": f"F-{n:06d}", "advisor_id": adv, "office_id": office, "fund_category": cat,
            "month": mo.isoformat(), "aum": round(base, 2), "cash_flow": round(base * rng.uniform(-0.02, 0.03), 2),
        })
        n += 1

idx = list(range(len(aum)))
used = set()

f1 = pick(idx, 20, used); used |= f1
for i in f1:
    aum[i]["aum"] = rng.choice(["N/A", "1,234,567.00", "TBD"])  # schema violation: string in a numeric field
    planted.append(("F1", "aum", "record_id", aum[i]["record_id"], "Schema violation: non-numeric AUM"))

f2 = pick(idx, 25, used); used |= f2
for i in f2:
    aum[i]["advisor_id"] = None
    planted.append(("F2", "aum", "record_id", aum[i]["record_id"], "Missing advisor_id (unique id null)"))

cur_rows = [i for i in idx if aum[i]["month"] == CUR.isoformat() and i not in used]
f3 = pick(cur_rows, 5, used); used |= f3
for i in f3:
    aum[i]["aum"] = round(aum[i]["aum"] * 6, 2)
    planted.append(("F3", "aum", "record_id", aum[i]["record_id"], "Advisor AUM spike vs historical average"))

f4 = pick(idx, 15, used); used |= f4
for k, i in enumerate(sorted(f4)):
    dup = dict(aum[i], record_id=f"F-D{k:05d}")
    aum.append(dup)
    planted.append(("F4", "aum", "advisor_month", f"{dup['advisor_id']}|{dup['month']}", "Duplicate advisor-month identifier"))

# COMMAND ----------

# MAGIC %md ## File manifest (timeliness checks)

# COMMAND ----------

manifest = [
    {"file_name": "activities_full.json", "source": "activities", "load_type": "full", "landed_at": (NOW - dt.timedelta(hours=30)).isoformat()},
    {"file_name": "credit_items_incr.json", "source": "activities", "load_type": "incremental", "landed_at": (NOW - dt.timedelta(hours=2)).isoformat()},
    {"file_name": "territories_incr.txt", "source": "territory", "load_type": "incremental", "landed_at": (NOW - dt.timedelta(hours=26)).isoformat()},
    {"file_name": "crm_contacts_incr.json", "source": "contacts", "load_type": "incremental", "landed_at": (NOW - dt.timedelta(hours=1)).isoformat()},
    {"file_name": "marketing_contacts_full.json", "source": "contacts", "load_type": "full", "landed_at": (NOW - dt.timedelta(hours=5)).isoformat()},
    {"file_name": "vendor_contacts_full.json", "source": "contacts", "load_type": "full", "landed_at": (NOW - dt.timedelta(hours=8)).isoformat()},
    {"file_name": "aum_cf_monthly.json", "source": "aum", "load_type": "full", "landed_at": (NOW - dt.timedelta(hours=3)).isoformat()},
    {"file_name": "fund_categories.json", "source": "aum", "load_type": "full", "landed_at": (NOW - dt.timedelta(hours=11)).isoformat()},
]
for r in manifest:
    if dt.datetime.fromisoformat(r["landed_at"]) < NOW - dt.timedelta(hours=12):
        planted.append(("T1", "file_manifest", "file_name", r["file_name"], "File older than 12h SLA"))

# COMMAND ----------

# MAGIC %md ## Write raw JSON to the volume + ground truth table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")


def write_jsonl(name, rows, parts):
    d = f"{RAW}/{name}"
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    rng.shuffle(rows)
    size = -(-len(rows) // parts)
    for p in range(parts):
        with open(f"{d}/part-{p:03d}.json", "w") as fh:
            for r in rows[p * size:(p + 1) * size]:
                fh.write(json.dumps(r) + "\n")
    return len(rows)


counts = {
    "contacts": write_jsonl("contacts", contacts, 4),
    "activities": write_jsonl("activities", activities, 8),
    "aum": write_jsonl("aum", aum, 4),
    "file_manifest": write_jsonl("file_manifest", manifest, 1),
}

(spark.createDataFrame(planted, "rule_id string, table_name string, match_col string, match_value string, defect string")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.planted_defects"))

print(counts)
display(spark.sql(f"SELECT rule_id, table_name, defect, count(*) AS planted FROM {CATALOG}.{SCHEMA}.planted_defects GROUP BY ALL ORDER BY rule_id"))
