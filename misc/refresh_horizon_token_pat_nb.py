# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Horizon bearer token (PAT) — notebook
# MAGIC Mints a ~1h Horizon/Polaris OAuth token by exchanging a scoped PAT via the Polaris
# MAGIC `client_credentials` flow, writes it to the Databricks secret the Bearer-mode IRC
# MAGIC connection references (`bearer_token = secret(<out_secret_scope>, <out_secret_key>)`),
# MAGIC then verifies with a federated `COUNT(*)`. This notebook never modifies the connection.
# MAGIC
# MAGIC **Inputs** are widgets. The **scoped PAT** is read from a Databricks secret
# MAGIC (notebooks can read secret values), so no PAT file is needed on disk.
# MAGIC
# MAGIC **One-time setup**
# MAGIC - Store the PAT (scoped to `sf_role`):
# MAGIC   `databricks secrets put-secret <pat_secret_scope> <pat_secret_key> --string-value "<PAT>"`
# MAGIC - Point the connection at the output secret (SQL, once):
# MAGIC   `ALTER CONNECTION <conn> OPTIONS (uri '…', bearer_token secret('<out_secret_scope>','<out_secret_key>'))`
# MAGIC - The runner needs **WRITE** on the output scope and **READ** on the PAT scope.

# COMMAND ----------
# MAGIC %pip install databricks-sdk
# MAGIC %restart_python

# COMMAND ----------
# ---- Configuration: set these for your environment ----
SF_ACCOUNT     = "ZPVXQGA-JC56578"             # Snowflake account (e.g. ORG-ACCOUNT)
SF_ROLE        = "DATABRICKS_FED_ROLE"         # Snowflake/Polaris role for the token scope
PAT_SCOPE, PAT_KEY = "horizon", "fed_pat"      # secret holding the scoped PAT
OUT_SCOPE, OUT_KEY = "horizon", "bearer_token" # secret to write the bearer token into
TOKEN_ENDPOINT = ""                            # "" to derive from SF_ACCOUNT (Polaris endpoint)
VERIFY_TABLE   = "bg_sf_iceberg_rest.tpch.region"  # "" to skip verification
VERIFY_EXPECT  = "5"                           # expected row count

for _n in ("SF_ACCOUNT", "SF_ROLE", "PAT_SCOPE", "PAT_KEY", "OUT_SCOPE", "OUT_KEY"):
    assert globals()[_n], f"{_n} is required"
TOKEN_ENDPOINT = TOKEN_ENDPOINT or \
    f"https://{SF_ACCOUNT.lower()}.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens"

# COMMAND ----------
# 1) exchange the scoped PAT for a Horizon token (client_credentials)
import requests
pat = dbutils.secrets.get(PAT_SCOPE, PAT_KEY)
r = requests.post(TOKEN_ENDPOINT,
                  headers={"Content-Type": "application/x-www-form-urlencoded"},
                  data={"grant_type": "client_credentials",
                        "client_id": "",                     # empty: Polaris identifies the principal from the PAT
                        "client_secret": pat,
                        "scope": f"session:role:{SF_ROLE}"}, timeout=30)
assert r.status_code == 200, f"exchange failed HTTP {r.status_code}: {r.text[:200]}"
tok = r.json()["access_token"]
print(f"minted {len(tok)} chars (expires_in={r.json().get('expires_in')})")

# COMMAND ----------
# 2) write the token into the secret the connection reads
from databricks.sdk import WorkspaceClient
WorkspaceClient().secrets.put_secret(scope=OUT_SCOPE, key=OUT_KEY, string_value=tok)
print(f"wrote secret {OUT_SCOPE}/{OUT_KEY}")

# COMMAND ----------
# 3) verify (optional): a federated COUNT(*) over the connection, which reads the refreshed secret.
# Note: if this cluster already cached the old credential, run the check on fresh compute.
if VERIFY_TABLE and VERIFY_EXPECT:
    cnt = spark.sql(f"SELECT count(*) FROM {VERIFY_TABLE}").collect()[0][0]
    ok = str(cnt) == VERIFY_EXPECT
    print(f"row count = {cnt} (expected {VERIFY_EXPECT}) -> " +
          ("OK: bearer catalog is live" if ok else "UNEXPECTED (see above)"))
else:
    print("verification skipped (verify_table / verify_expect blank)")
