"""REST connector for Lakeflow Connect.

Ingests the demo tables from the FastAPI service that fronts the
Demo SQL Server database. Table discovery, schemas, and rows are all driven
by the REST API (no hard-coded schemas), so new tables appear automatically.

REST contract:
    GET /tables                    -> {"tables": [...]}
    GET /tables/{name}/schema      -> {"columns":[{name,type,nullable}],
                                       "primary_keys":[...], "cursor_field": null}
        type tokens: string | int | long | decimal(p,s) | date | datetime | bool
    GET /tables/{name}?limit=&after=  -> {"rows":[...], "next":{"after":<pk>}|null}
"""

import re
import time
from typing import Iterator

import requests
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

_PAGE = 1000
_RETRIABLE = {429, 500, 502, 503, 504}
_MAX_RETRIES = 5


def _spark_type(token: str):
    """Map a normalized API type token to a Spark DataType."""
    t = token.lower().strip()
    if t == "int":
        return IntegerType()
    if t == "long":
        return LongType()
    if t == "date":
        return DateType()
    if t == "datetime":
        return TimestampType()
    if t == "bool":
        return BooleanType()
    if t.startswith("decimal"):
        m = re.match(r"decimal\((\d+)\s*,\s*(\d+)\)", t)
        return DecimalType(int(m.group(1)), int(m.group(2))) if m else DecimalType(38, 10)
    return StringType()


class RestLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation over the REST API."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        base_url = options.get("base_url") or options.get("host")
        token = options.get("token") or options.get("bearer_token")
        if not base_url:
            raise ValueError("REST connector requires 'base_url' in options")
        if not token:
            raise ValueError("REST connector requires 'token' in options")
        self._base = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        )

    # --- HTTP -------------------------------------------------------------
    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        backoff = 1.0
        for attempt in range(_MAX_RETRIES):
            resp = self._session.get(url, params=params, timeout=30)
            if resp.status_code not in _RETRIABLE:
                if resp.status_code != 200:
                    raise RuntimeError(f"GET {path} -> {resp.status_code}: {resp.text[:300]}")
                return resp.json()
            if attempt < _MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError(f"GET {path} failed after {_MAX_RETRIES} retries")

    # --- discovery --------------------------------------------------------
    def list_tables(self) -> list[str]:
        return list(self._get("/tables")["tables"])

    def _validate_table(self, table_name: str) -> None:
        supported = self.list_tables()
        if table_name not in supported:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {supported}"
            )

    def _schema_doc(self, table_name: str) -> dict:
        return self._get(f"/tables/{table_name}/schema")

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        doc = self._schema_doc(table_name)
        return StructType(
            [
                StructField(c["name"], _spark_type(c["type"]), bool(c.get("nullable", True)))
                for c in doc["columns"]
            ]
        )

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        doc = self._schema_doc(table_name)
        primary_keys = doc.get("primary_keys") or []
        if not primary_keys:
            # Keyless source table (e.g. data_dictionary): use the full column set
            # as a composite key so snapshot SCD_TYPE_1 upserts dedup identical rows.
            primary_keys = [c["name"] for c in doc["columns"]]
        return {
            "primary_keys": primary_keys,
            "cursor_field": doc.get("cursor_field"),
            # Full-refresh snapshot each run — the REST source has no change feed.
            "ingestion_type": "snapshot",
        }

    # --- read -------------------------------------------------------------
    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Snapshot read: page through the whole table via keyset pagination.

        Reads all rows in this call and returns an empty offset (snapshot has
        nothing to resume).
        """
        self._validate_table(table_name)  # eager: raise before returning the generator
        limit = int((table_options or {}).get("limit", _PAGE))

        def rows() -> Iterator[dict]:
            after = None
            while True:
                params = {"limit": limit}
                if after is not None:
                    params["after"] = after
                page = self._get(f"/tables/{table_name}", params)
                batch = page.get("rows", [])
                for row in batch:
                    yield row
                nxt = page.get("next")
                if not nxt or nxt.get("after") is None:
                    break
                after = nxt["after"]

        return rows(), {}
