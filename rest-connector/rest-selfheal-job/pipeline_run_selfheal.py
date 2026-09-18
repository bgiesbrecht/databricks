#!/usr/bin/env python3
"""Self-healing runner for a Lakeflow ingestion pipeline.

Runs an INCREMENTAL update. If it fails specifically on a BREAKING schema change
(incompatible type change / rename ambiguity -> SAAS_INCOMPATIBLE_SCHEMA_CHANGES),
it automatically retries once with a full refresh of ONLY the affected table(s)
(identified from the failing event's origin / error text), falling back to a
full refresh of all tables if the table can't be pinned down. Any OTHER failure
is surfaced as-is (never auto-refreshed) so real errors aren't masked.

Exit 0 on success, non-zero on unrecoverable failure. Runs locally or as a
Databricks Job (Python task). Uses the Databricks SDK.

Usage:
  python pipeline_run_selfheal.py --pipeline-id <id> [--profile P]
      [--no-auto-refresh] [--timeout 1800] [--webhook URL]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.request

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import UpdateInfoState

TERMINAL = {UpdateInfoState.COMPLETED, UpdateInfoState.FAILED, UpdateInfoState.CANCELED}

# Error signatures a full refresh is known to resolve (breaking schema changes).
# Matched case-insensitively. "full refresh" catches the generic remediation hint
# Databricks emits for recoverable schema conflicts (rename ambiguity, type changes).
BREAKING_SIGNATURES = (
    "incompatible_schema",
    "could have been renamed",
    "full refresh",
)


def start(w, pid, full_refresh=False, selection=None):
    """Start an update: selective full refresh if `selection` given, else all/incremental."""
    if selection:
        print(f">> starting SELECTIVE full refresh of: {sorted(selection)}")
        return w.pipelines.start_update(
            pipeline_id=pid, full_refresh_selection=list(selection)).update_id
    kind = "FULL REFRESH (all)" if full_refresh else "incremental"
    print(f">> starting {kind} update")
    return w.pipelines.start_update(pipeline_id=pid, full_refresh=full_refresh).update_id


def wait(w, pid, update_id, timeout):
    deadline, last = time.time() + timeout, None
    while time.time() < deadline:
        state = w.pipelines.get_update(pipeline_id=pid, update_id=update_id).update.state
        if state != last:
            print(f"   update {update_id[:8]}: {state.value}")
            last = state
        if state in TERMINAL:
            return state
        time.sleep(15)
    raise TimeoutError(f"update {update_id} did not reach a terminal state in {timeout}s")


def failure_reason(w, pid, update_id):
    """First exception message for the given update (newest events first), else ''.

    When update_id is given, only events from that update (or update-less events)
    are considered; when falsy, the newest exception on the pipeline is returned.
    """
    for e in w.pipelines.list_pipeline_events(pipeline_id=pid):
        origin = getattr(e, "origin", None)
        if update_id and origin and getattr(origin, "update_id", None) not in (None, update_id):
            continue
        err = getattr(e, "error", None)
        excs = getattr(err, "exceptions", None) if err else None
        if excs:
            return excs[0].message or ""
    return ""


def failed_tables(w, pid, update_id, reason):
    """Identify which table(s) failed, so we can full-refresh only those.

    Primary source: the failing event's `origin` (dataset_name / flow_name /
    ingestion_source_table_name). Fallback: parse the table name out of the error
    message ("of table `X`" for renames, "columns of X" for type changes).
    """
    tables = set()
    for e in w.pipelines.list_pipeline_events(pipeline_id=pid):
        origin = getattr(e, "origin", None)
        if not origin:
            continue
        if update_id and getattr(origin, "update_id", None) not in (None, update_id):
            continue
        err = getattr(e, "error", None)
        if not (getattr(err, "exceptions", None) if err else None):
            continue
        for attr in ("dataset_name", "flow_name", "ingestion_source_table_name",
                     "materialization_name"):
            val = getattr(origin, attr, None)
            if val:
                tables.add(val)
                break
    # Message fallbacks (both known breaking-error phrasings name the table).
    for pat in (r"of table `([^`]+)`", r"columns of (\w+)"):
        tables.update(re.findall(pat, reason or ""))
    return {t for t in tables if t}


def notify(webhook, text):
    if not webhook:
        return
    try:
        req = urllib.request.Request(
            webhook, data=json.dumps({"text": text}).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:  # noqa: BLE001 - alerting must never crash the run
        print(f"   (notify failed: {exc})")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pipeline-id", required=True)
    ap.add_argument("--profile", default=None, help="Databricks CLI profile")
    ap.add_argument("--no-auto-refresh", action="store_true",
                    help="detect + alert on breaking changes but do NOT auto full-refresh")
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--webhook", default=None, help="optional Slack/webhook URL for alerts")
    a = ap.parse_args()

    # In a Databricks Job the default auth is the job's context; --profile is for local runs.
    w = WorkspaceClient(profile=a.profile) if a.profile else WorkspaceClient()

    uid1 = start(w, a.pipeline_id, False)
    state = wait(w, a.pipeline_id, uid1, a.timeout)
    if state == UpdateInfoState.COMPLETED:
        print("OK: incremental update completed.")
        return 0

    reason = failure_reason(w, a.pipeline_id, uid1)
    print(f"incremental update {state.value}: {reason[:300]}")
    breaking = any(sig in reason.lower() for sig in BREAKING_SIGNATURES)

    if state == UpdateInfoState.FAILED and breaking and not a.no_auto_refresh:
        tables = failed_tables(w, a.pipeline_id, uid1, reason)
        if tables:
            notify(a.webhook, f"pipeline breaking schema change on {sorted(tables)}; full-refreshing those tables only. {reason[:150]}")
            uid2 = start(w, a.pipeline_id, selection=tables)
        else:
            # Couldn't pinpoint the table — over-recover rather than stay broken.
            notify(a.webhook, f"pipeline breaking schema change (table unknown); full-refreshing ALL. {reason[:150]}")
            uid2 = start(w, a.pipeline_id, full_refresh=True)
        state2 = wait(w, a.pipeline_id, uid2, a.timeout)
        if state2 == UpdateInfoState.COMPLETED:
            scope = f"tables {sorted(tables)}" if tables else "all tables"
            print(f"OK: full refresh of {scope} recovered the breaking schema change.")
            notify(a.webhook, f"pipeline recovered via full refresh of {scope}.")
            return 0
        r2 = failure_reason(w, a.pipeline_id, uid2)
        print(f"full refresh also {state2.value}: {r2[:300]}")
        notify(a.webhook, f"pipeline full refresh FAILED: {r2[:200]}")
        return 1

    # Non-breaking failure, or auto-refresh disabled: surface it, don't mask it.
    notify(a.webhook, f"pipeline failed (not auto-recovered): {reason[:200]}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
