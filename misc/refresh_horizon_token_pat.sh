#!/usr/bin/env bash
# Refresh the ~1-hour Horizon OAuth access token for a Bearer-mode IRC connection,
# minting it from a scoped PAT via the Polaris client_credentials exchange.
# One command: re-mint the token, write it to a Databricks secret, and (optionally) verify.
#
# This is the PAT counterpart to refresh_horizon_token.sh (which uses a key-pair). It runs
# the same client_credentials exchange the M2M connection does under the hood.
#
# The connection is assumed to already reference the Databricks secret for its bearer_token
# (bearer_token = secret(SECRET_SCOPE, SECRET_KEY)). Each run mints a fresh token and
# overwrites that secret; the connection reads the new value on next use. This script never
# modifies the connection itself.
#
# Required env vars:
#   SF_ACCOUNT                   Snowflake account identifier (e.g. ORG-ACCOUNT)
#   SF_ROLE                      Snowflake/Polaris role used for the token scope
#   SF_PAT_FILE                  path to a file holding a PAT scoped to SF_ROLE
#   UC_CONNECTION                Unity Catalog connection name that serves the token
#   DATABRICKS_PROFILE           configured Databricks CLI profile
#   SECRET_SCOPE  SECRET_KEY     Databricks secret the connection reads for bearer_token
# Required for verify (step 3; skip with --no-verify or VERIFY=0):
#   DATABRICKS_SQL_WAREHOUSE_ID  SQL warehouse used to run the check query
#   VERIFY_TABLE  VERIFY_EXPECT  table to COUNT(*) and its expected row count
# Optional:
#   HORIZON_URI     Polaris catalog URI (default https://$SF_ACCOUNT.snowflakecomputing.com/polaris/api/catalog)
#   TOKEN_ENDPOINT  OAuth token endpoint (default $HORIZON_URI/v1/oauth/tokens)
#
# Prereqs: the secret scope exists and the connection references it; a network policy covers
# the PAT's user; and: pip install requests
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"      # -> repo root containing tools/

: "${SF_ACCOUNT:?set SF_ACCOUNT (Snowflake account, e.g. ORG-ACCOUNT)}"
: "${SF_ROLE:?set SF_ROLE (Snowflake/Polaris role for the token scope)}"
: "${SF_PAT_FILE:?set SF_PAT_FILE (path to a file holding a PAT scoped to SF_ROLE)}"
: "${UC_CONNECTION:?set UC_CONNECTION (Unity Catalog connection name)}"
: "${DATABRICKS_PROFILE:?set DATABRICKS_PROFILE (Databricks CLI profile)}"
: "${SECRET_SCOPE:?set SECRET_SCOPE (Databricks secret scope the connection reads)}"
: "${SECRET_KEY:?set SECRET_KEY (Databricks secret key the connection reads)}"

ACCT="$SF_ACCOUNT"
ROLE="$SF_ROLE"
PAT_FILE="$SF_PAT_FILE"
CONN="$UC_CONNECTION"
PROFILE="$DATABRICKS_PROFILE"
URI="${HORIZON_URI:-https://$ACCT.snowflakecomputing.com/polaris/api/catalog}"
TOKEN_ENDPOINT="${TOKEN_ENDPOINT:-$URI/v1/oauth/tokens}"
VERIFY="${VERIFY:-1}"                       # VERIFY=0 or --no-verify skips step 3
for arg in "$@"; do
  case "$arg" in
    --no-verify) VERIFY=0 ;;
  esac
done

echo "[1/3] exchanging scoped PAT for a fresh Horizon token (client_credentials) ..."
TOKEN=$(python3 - "$TOKEN_ENDPOINT" "$ROLE" "$PAT_FILE" <<'PY'
import sys, requests
endpoint, role, pat_file = sys.argv[1:4]
pat = open(pat_file).read().strip()
r = requests.post(endpoint, headers={"Content-Type": "application/x-www-form-urlencoded"},
                  data={"grant_type": "client_credentials",
                        "client_id": "",                      # empty: Polaris identifies the principal from the PAT
                        "client_secret": pat,
                        "scope": f"session:role:{role}"}, timeout=30)
assert r.status_code == 200, f"exchange failed HTTP {r.status_code}: {r.text[:200]}"
tok = r.json()["access_token"]
print(tok)                                   # stdout: token only (captured by the shell)
print(f"      minted {len(tok)} chars (expires_in={r.json().get('expires_in')})", file=sys.stderr)
PY
)

echo "[2/3] writing token to secret $SECRET_SCOPE/$SECRET_KEY ..."
databricks secrets put-secret "$SECRET_SCOPE" "$SECRET_KEY" --string-value "$TOKEN" -p "$PROFILE"
echo "      secret updated (connection reads it on next use)"

# [3/3] verify (optional): run COUNT(*) on VERIFY_TABLE through the Databricks SQL warehouse
# (DATABRICKS_SQL_WAREHOUSE_ID) over the connection, which reads the refreshed secret, and
# check it equals VERIFY_EXPECT. A match confirms the new token can read the federated catalog
# end to end. Skip with VERIFY=0 or --no-verify (e.g. in CI, or when the warehouse is asleep).
if [ "$VERIFY" = "1" ]; then
  : "${DATABRICKS_SQL_WAREHOUSE_ID:?set DATABRICKS_SQL_WAREHOUSE_ID to verify, or pass --no-verify}"
  : "${VERIFY_TABLE:?set VERIFY_TABLE to verify, or pass --no-verify}"
  : "${VERIFY_EXPECT:?set VERIFY_EXPECT to verify, or pass --no-verify}"
  echo "[3/3] verifying (row count of $VERIFY_TABLE) ..."
  R=$(databricks api post /api/2.0/sql/statements -p "$PROFILE" \
       --json "{\"warehouse_id\":\"$DATABRICKS_SQL_WAREHOUSE_ID\",\"statement\":\"SELECT count(*) FROM $VERIFY_TABLE\",\"wait_timeout\":\"50s\"}" 2>/dev/null \
     | jq -r '.result.data_array[0][0] // (.status.state + ": " + (.status.error.message // ""))')
  echo "      row count = $R"
  [ "$R" = "$VERIFY_EXPECT" ] && echo "token refreshed using PAT: Bearer catalog is live" || echo "unexpected result (see above)"
else
  echo "[3/3] verification skipped (VERIFY=0): token minted and secret updated"
fi
