# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Horizon bearer token (key-pair) — notebook
# MAGIC Mints a ~1h Horizon/Polaris OAuth token from a key-pair JWT and writes it to the
# MAGIC Databricks secret the Bearer-mode IRC connection references
# MAGIC (`bearer_token = secret(<out_secret_scope>, <out_secret_key>)`), then verifies with a
# MAGIC federated `COUNT(*)`. This notebook never modifies the connection.
# MAGIC
# MAGIC **Inputs** are widgets. The **RSA private key** is read from a Databricks secret
# MAGIC (notebooks can read secret values), so no key file is needed on disk.
# MAGIC
# MAGIC **One-time setup**
# MAGIC - Store the private key:
# MAGIC   `databricks secrets put-secret <key_secret_scope> <key_secret_key> --string-value "$(cat ~/.snowflake/rsa_key.p8)"`
# MAGIC - Point the connection at the output secret (SQL, once):
# MAGIC   `ALTER CONNECTION <conn> OPTIONS (uri '…', bearer_token secret('<out_secret_scope>','<out_secret_key>'))`
# MAGIC - The runner needs **WRITE** on the output scope and **READ** on the key scope.

# COMMAND ----------
# MAGIC %pip install pyjwt cryptography databricks-sdk
# MAGIC %restart_python

# COMMAND ----------
# ---- Configuration: set these for your environment ----
SF_ACCOUNT     = "ZPVXQGA-JC56578"             # Snowflake account (e.g. ORG-ACCOUNT)
SF_FED_SERVICE = "DATABRICKS_FED_SVC"          # Snowflake service user for the JWT
SF_ROLE        = "DATABRICKS_FED_ROLE"         # Snowflake/Polaris role for the token scope
KEY_SCOPE, KEY_KEY = "horizon", "rsa_key"      # secret holding the RSA private key PEM
OUT_SCOPE, OUT_KEY = "horizon", "bearer_token" # secret to write the bearer token into
VERIFY_TABLE   = "bg_sf_iceberg_rest.tpch.region"  # "" to skip verification
VERIFY_EXPECT  = "5"                           # expected row count

for _n in ("SF_ACCOUNT", "SF_FED_SERVICE", "SF_ROLE", "KEY_SCOPE", "KEY_KEY", "OUT_SCOPE", "OUT_KEY"):
    assert globals()[_n], f"{_n} is required"

# COMMAND ----------
# 1) build a key-pair JWT and exchange it for a Horizon token
import base64, hashlib, requests, jwt
from datetime import datetime, timezone, timedelta
from cryptography.hazmat.primitives import serialization

private_key = serialization.load_pem_private_key(
    dbutils.secrets.get(KEY_SCOPE, KEY_KEY).encode(), password=None)

acct = SF_ACCOUNT.upper().split(".")[0]
qualified = f"{acct}.{SF_FED_SERVICE.upper()}"
pub_der = private_key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
fp = "SHA256:" + base64.b64encode(hashlib.sha256(pub_der).digest()).decode()
now = datetime.now(timezone.utc)
assertion = jwt.encode(
    {"iss": f"{qualified}.{fp}", "sub": qualified, "iat": now, "exp": now + timedelta(minutes=59)},
    private_key, algorithm="RS256")

host = f"{SF_ACCOUNT.lower()}.snowflakecomputing.com"
scope = f"session:role:{SF_ROLE}"
tok = None
for data in ({"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion, "scope": scope},
             {"grant_type": "client_credentials", "client_secret": assertion, "scope": scope}):
    r = requests.post(f"https://{host}/oauth/token", data=data,
                      headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=30)
    print(f"[oauth] grant={data['grant_type'].split(':')[-1]} -> HTTP {r.status_code}")
    if r.status_code == 200:
        tok = r.text.strip()
        tok = r.json().get("access_token", tok) if tok.startswith("{") else tok
        break
assert tok, "token mint failed (see HTTP status above)"
print(f"minted {len(tok)} chars")

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
