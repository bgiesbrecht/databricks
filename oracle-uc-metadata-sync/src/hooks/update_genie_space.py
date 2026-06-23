"""
update_genie_space.py — Databricks Genie Spaces SDK (single-file).

A reusable, configurable Python SDK for managing Databricks Genie Spaces
via the documented public Genie API:

    GET    /api/2.0/genie/spaces?include_serialized_space=true
    GET    /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
    POST   /api/2.0/genie/spaces
    PATCH  /api/2.0/genie/spaces/{space_id}
    DELETE /api/2.0/genie/spaces/{space_id}    (soft-delete to trash)

The space's configuration lives inside a single `serialized_space` JSON
string. Editing pattern is GET → parse → mutate → re-serialize → PATCH.
The etag returned from GET must be echoed back on PATCH (optimistic
concurrency).

Library use:

    from update_genie_space import GenieSpaces

    client = GenieSpaces(
        host="https://workspace.cloud.databricks.com",
        token="<PAT or OAuth access token>",   # or a callable returning one
    )

    inner = client.dump(space_id)              # read the full config dict
    client.apply_config(space_id, my_config)   # update from a portable dict
    new = client.create(                       # provision a fresh space
        warehouse_id="<id>",
        table_identifiers=["catalog.schema.table"],
        config=my_config,
    )
    client.delete(new["space_id"])             # soft-delete to trash

CLI:
    DATABRICKS_HOST=... DATABRICKS_TOKEN=... GENIE_SPACE_ID=... \\
        python update_genie_space.py <config.json>

See README.md for the portable config schema, validator quirks, and how
Genie reads each section.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Callable, Union

import requests


# ── Inner-doc constants and helpers (pure dict manipulation, no I/O) ─────────

SNIPPET_KIND_FILTER     = "filters"
SNIPPET_KIND_EXPRESSION = "expressions"


def _new_id() -> str:
    """Lowercase 32-hex UUID without hyphens — required by the PATCH validator."""
    return uuid.uuid4().hex


def _ins(inner: dict) -> dict:
    """Return the `instructions` sub-dict, creating it if missing."""
    return inner.setdefault("instructions", {})


def _sort_by_id(lst: list) -> list:
    """The PATCH validator rejects id-bearing lists unless they are sorted by id."""
    return sorted(lst, key=lambda x: x.get("id", ""))


def _normalize_inner(inner: dict) -> dict:
    """
    Sort all id-bearing lists by id (required by the PATCH validator).
    Also sorts data_sources.tables by identifier and column_configs by column_name.
    Operates in place and returns the same dict for convenience.
    """
    ins = inner.get("instructions", {})
    for k in ("text_instructions", "example_question_sqls", "join_specs"):
        if k in ins:
            ins[k] = _sort_by_id(ins[k])
    snip = ins.get("sql_snippets", {})
    for k in ("filters", "expressions"):
        if k in snip:
            snip[k] = _sort_by_id(snip[k])
    cfg = inner.get("config", {})
    if "sample_questions" in cfg:
        cfg["sample_questions"] = _sort_by_id(cfg["sample_questions"])

    ds = inner.get("data_sources", {})
    if "tables" in ds:
        ds["tables"] = sorted(ds["tables"], key=lambda t: t.get("identifier", ""))
        for t in ds["tables"]:
            if "column_configs" in t:
                t["column_configs"] = sorted(t["column_configs"],
                                             key=lambda c: c.get("column_name", ""))
    return inner


# ── Inner-doc mutators ───────────────────────────────────────────────────────
#
# These operate on a parsed `inner` dict (json.loads(serialized_space)) in
# place. They hide the schema quirks: every list item gets an `id` (32-hex
# UUID), every string-bearing field is wrapped in a single-element list,
# join relationship_type is encoded as the `--rt=...--` SQL-comment annotation,
# etc. They do NOT touch the network — call client.patch() after mutating.

def set_text_instruction(inner: dict, content: str) -> None:
    """
    Replace the single allowed text instruction. The server caps this at 1
    entry; consolidate vocabulary, defaults, output rules into one string.
    """
    _ins(inner)["text_instructions"] = [{"id": _new_id(), "content": [content]}]


def replace_example_question_sqls(inner: dict,
                                  items: "list[tuple[str, str]]") -> None:
    """Replace all worked SQL examples with the given (question, sql) pairs."""
    _ins(inner)["example_question_sqls"] = [
        {"id": _new_id(), "question": [q], "sql": [sql]} for q, sql in items
    ]


def add_example_question_sql(inner: dict, question: str, sql: str) -> None:
    """Append (or upsert by exact question match) one worked SQL example."""
    lst = _ins(inner).setdefault("example_question_sqls", [])
    for item in lst:
        if item.get("question") == [question]:
            item["sql"] = [sql]
            return
    lst.append({"id": _new_id(), "question": [question], "sql": [sql]})


def make_join_spec(left_table: str, left_alias: str,
                   right_table: str, right_alias: str,
                   join_on: str,
                   relationship_type: str = "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",
                   instruction: str = "") -> dict:
    """
    Build a join_specs entry. relationship_type is encoded as a magic SQL
    comment appended to the `sql` list (`--rt=FROM_RELATIONSHIP_TYPE_*--`).
    Valid values: MANY_TO_ONE, ONE_TO_MANY, ONE_TO_ONE, MANY_TO_MANY.
    """
    return {
        "id":    _new_id(),
        "left":  {"identifier": left_table,  "alias": left_alias},
        "right": {"identifier": right_table, "alias": right_alias},
        "sql":   [join_on, f"--rt={relationship_type}--"],
        "instruction": [instruction],
    }


def replace_join_specs(inner: dict, specs: list) -> None:
    """Replace all join_specs. Each spec is the dict returned by make_join_spec."""
    _ins(inner)["join_specs"] = list(specs)


def add_join_spec(inner: dict,
                  left_table: str, left_alias: str,
                  right_table: str, right_alias: str,
                  join_on: str,
                  relationship_type: str = "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",
                  instruction: str = "") -> None:
    """
    Append (or upsert by alias-pair) one join spec. Match key is the four-tuple
    (left_table, left_alias, right_table, right_alias).
    """
    lst = _ins(inner).setdefault("join_specs", [])
    key = (left_table, left_alias, right_table, right_alias)
    new_spec = make_join_spec(left_table, left_alias, right_table, right_alias,
                              join_on, relationship_type, instruction)
    for i, item in enumerate(lst):
        existing = (item.get("left", {}).get("identifier"),
                    item.get("left", {}).get("alias"),
                    item.get("right", {}).get("identifier"),
                    item.get("right", {}).get("alias"))
        if existing == key:
            new_spec["id"] = item.get("id", new_spec["id"])
            lst[i] = new_spec
            return
    lst.append(new_spec)


def make_sql_snippet(display_name: str, sql: str,
                     instruction: str = "",
                     synonyms: "list[str] | None" = None) -> dict:
    """Build one sql_snippet entry (used for both filters and expressions)."""
    return {
        "id":           _new_id(),
        "display_name": display_name,
        "sql":          [sql],
        "instruction":  [instruction],
        "synonyms":     synonyms or [],
    }


def replace_sql_snippets(inner: dict, kind: str, items: list) -> None:
    """
    Replace one bucket of sql_snippets — `SNIPPET_KIND_FILTER` or
    `SNIPPET_KIND_EXPRESSION`. Each item is the dict returned by make_sql_snippet.
    """
    snip = _ins(inner).setdefault("sql_snippets", {"filters": [], "expressions": []})
    snip[kind] = list(items)


def add_sql_snippet(inner: dict, kind: str,
                    display_name: str, sql: str,
                    instruction: str = "",
                    synonyms: "list[str] | None" = None) -> None:
    """
    Append (or upsert by display_name) one sql_snippet into either the
    'filters' or 'expressions' bucket.
    """
    snip = _ins(inner).setdefault("sql_snippets", {"filters": [], "expressions": []})
    bucket = snip.setdefault(kind, [])
    new_item = make_sql_snippet(display_name, sql, instruction, synonyms)
    for i, item in enumerate(bucket):
        if item.get("display_name") == display_name:
            new_item["id"] = item.get("id", new_item["id"])
            bucket[i] = new_item
            return
    bucket.append(new_item)


def replace_sample_questions(inner: dict, questions: "list[str]") -> None:
    """Replace the suggested-question chips shown in the UI."""
    inner.setdefault("config", {})["sample_questions"] = [
        {"id": _new_id(), "question": [q]} for q in questions
    ]


def add_sample_question(inner: dict, question: str) -> None:
    """
    Append one suggested-question chip if not already present (matched by
    exact question text). No-op if already on the list.
    """
    lst = inner.setdefault("config", {}).setdefault("sample_questions", [])
    for item in lst:
        if item.get("question") == [question]:
            return
    lst.append({"id": _new_id(), "question": [question]})


# ── Portable config → inner-doc applier ──────────────────────────────────────
#
# The portable config dict is more ergonomic than the raw serialized_space:
# plain strings (not [str] lists), no required ids, no manual sort order.
# Sections are all optional — omitted keys leave that section untouched.
#
#   {
#     "text_instruction": "<single string>",
#     "examples": [{"question": "...", "sql": "..."}],
#     "joins": [{
#       "left":  {"table": "<fqn>", "alias": "<a>"},
#       "right": {"table": "<fqn>", "alias": "<a>"},
#       "on": "<sql on-clause>",
#       "relationship_type": "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",  # optional
#       "instruction": "..."                                        # optional
#     }],
#     "sql_filters":     [{"display_name": "...", "sql": "...",
#                          "instruction": "...", "synonyms": ["..."]}],
#     "sql_expressions": [...],   # same shape as sql_filters
#     "sample_questions": ["...", "..."]
#   }

def apply_config_to_inner(inner: dict, config: dict) -> None:
    """
    Apply a portable config dict's sections to a parsed inner dict in place.
    Only sections present in `config` are touched.
    """
    if "text_instruction" in config:
        set_text_instruction(inner, config["text_instruction"])

    if "examples" in config:
        replace_example_question_sqls(
            inner,
            [(e["question"], e["sql"]) for e in config["examples"]],
        )

    if "joins" in config:
        replace_join_specs(inner, [
            make_join_spec(
                left_table=j["left"]["table"],   left_alias=j["left"]["alias"],
                right_table=j["right"]["table"], right_alias=j["right"]["alias"],
                join_on=j["on"],
                relationship_type=j.get("relationship_type",
                                        "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE"),
                instruction=j.get("instruction", ""),
            )
            for j in config["joins"]
        ])

    if "sql_filters" in config:
        replace_sql_snippets(inner, SNIPPET_KIND_FILTER, [
            make_sql_snippet(
                display_name=f["display_name"],
                sql=f["sql"],
                instruction=f.get("instruction", ""),
                synonyms=f.get("synonyms"),
            )
            for f in config["sql_filters"]
        ])

    if "sql_expressions" in config:
        replace_sql_snippets(inner, SNIPPET_KIND_EXPRESSION, [
            make_sql_snippet(
                display_name=e["display_name"],
                sql=e["sql"],
                instruction=e.get("instruction", ""),
                synonyms=e.get("synonyms"),
            )
            for e in config["sql_expressions"]
        ])

    if "sample_questions" in config:
        replace_sample_questions(inner, list(config["sample_questions"]))


# ── Client ───────────────────────────────────────────────────────────────────

TokenProvider = Union[str, Callable[[], str]]


class GenieSpacesError(Exception):
    """Raised when a Genie Spaces API call fails."""

    def __init__(self, method: str, path: str, status: int, body: str):
        self.method = method
        self.path   = path
        self.status = status
        self.body   = body
        super().__init__(f"{method} {path} -> HTTP {status}: {body[:500]}")


class GenieSpaces:
    """
    Client for the Databricks Genie Spaces API.

    Construct once with workspace host + auth, then call methods to manage
    individual spaces. Auth can be a fixed token string or a zero-arg callable
    that returns a fresh token (useful when wrapping a token-refreshing source).
    """

    API_PREFIX = "/api/2.0/genie/spaces"

    def __init__(self,
                 host: str,
                 token: TokenProvider,
                 *,
                 session: "requests.Session | None" = None,
                 timeout: float = 60.0):
        if not host:
            raise ValueError("host must not be empty")
        if token is None or (isinstance(token, str) and not token):
            raise ValueError("token must not be empty")
        self.host    = host.rstrip("/")
        self._token  = token
        self._session = session or requests.Session()
        self.timeout = timeout

    # ---------- low-level transport ----------

    def _get_token(self) -> str:
        return self._token() if callable(self._token) else self._token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}",
                "Content-Type":  "application/json"}

    def _request(self, method: str, path: str, *,
                 params: "dict | None" = None,
                 json_body: "dict | None" = None) -> dict:
        url = f"{self.host}{path}"
        resp = self._session.request(method, url,
                                     params=params,
                                     json=json_body,
                                     headers=self._headers(),
                                     timeout=self.timeout)
        if not resp.ok:
            raise GenieSpacesError(method, path, resp.status_code, resp.text)
        if not resp.content:
            return {}
        return resp.json()

    # ---------- CRUD ----------

    def get(self, space_id: str, *, include_serialized: bool = True) -> "tuple[dict, dict, str]":
        """
        GET the space.

        Returns (outer, inner, etag):
          outer = full server response (metadata + serialized_space string)
          inner = parsed dict from outer["serialized_space"] (or empty if not requested)
          etag  = pass back on the next patch() to detect concurrent edits
        """
        params = {"include_serialized_space": "true"} if include_serialized else None
        outer = self._request("GET", f"{self.API_PREFIX}/{space_id}", params=params)
        inner = json.loads(outer["serialized_space"]) if include_serialized else {}
        return outer, inner, outer.get("etag", "")

    def dump(self, space_id: str) -> dict:
        """Convenience: return only the parsed inner config (the canonical room shape)."""
        _, inner, _ = self.get(space_id)
        return inner

    def patch(self, space_id: str, inner: dict, etag: str) -> dict:
        """
        PATCH the space with a mutated inner dict. Runs the validator-required
        sort/normalize pass before sending. Returns the outer response, which
        carries the new etag.
        """
        _normalize_inner(inner)
        body = {"serialized_space": json.dumps(inner), "etag": etag}
        return self._request("PATCH", f"{self.API_PREFIX}/{space_id}", json_body=body)

    def apply_config(self, space_id: str, config: dict) -> dict:
        """
        Apply a portable config dict to an existing space in one
        GET → mutate → PATCH cycle. Returns the new outer response.
        """
        _, inner, etag = self.get(space_id)
        apply_config_to_inner(inner, config)
        return self.patch(space_id, inner, etag)

    def create(self,
               *,
               warehouse_id: str,
               table_identifiers: "list[str]",
               title: str = "",
               parent_path: str = "",
               description: str = "",
               config: "dict | None" = None) -> dict:
        """
        Create a brand-new Genie Space.

          warehouse_id:      SQL warehouse the space will run queries against.
          table_identifiers: tables to attach, each as a three-part FQN. Must
                             have at least one. Sorted alphabetically before send.
          title:             optional human-readable name; server defaults to
                             "New Space" and appends a timestamp on duplicates.
          parent_path:       optional workspace folder (e.g. "/Users/you@x.com").
          description:       optional description.
          config:            optional portable config dict (same shape as
                             apply_config). Sections supplied here are baked
                             into the initial serialized_space.

        Returns the outer response: {space_id, title, warehouse_id, etag, ...}.
        """
        if not table_identifiers:
            raise ValueError("create: table_identifiers must not be empty")

        inner = {
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": fqn} for fqn in sorted(table_identifiers)],
            },
        }
        if config:
            apply_config_to_inner(inner, config)
        _normalize_inner(inner)

        body = {"warehouse_id": warehouse_id, "serialized_space": json.dumps(inner)}
        if title:       body["title"]       = title
        if parent_path: body["parent_path"] = parent_path
        if description: body["description"] = description

        return self._request("POST", self.API_PREFIX, json_body=body)

    def delete(self, space_id: str) -> None:
        """
        Soft-delete. DELETE /api/2.0/genie/spaces/{space_id} moves the space
        to the workspace trash. Subsequent GETs return 404 ("has been trashed").
        Purge permanently from the workspace UI's Trash. Trash transition is
        eventually consistent — a GET right after delete may briefly return 200.
        """
        self._request("DELETE", f"{self.API_PREFIX}/{space_id}")

    def list(self, *, page_size: int = 100, max_spaces: "int | None" = None) -> "list[dict]":
        """
        List all (non-trashed) Genie Spaces in the workspace. Handles pagination
        via next_page_token. Pass max_spaces to cap the total returned.
        """
        all_spaces: list = []
        params: dict = {"page_size": page_size}
        while True:
            page = self._request("GET", self.API_PREFIX, params=params)
            all_spaces.extend(page.get("spaces", []))
            if max_spaces is not None and len(all_spaces) >= max_spaces:
                return all_spaces[:max_spaces]
            token = page.get("next_page_token")
            if not token:
                return all_spaces
            params = {"page_size": page_size, "page_token": token}


# ── CLI: apply a JSON config file to a single space ──────────────────────────

def _client_from_env() -> "tuple[GenieSpaces, str]":
    """Build a client + space_id from DATABRICKS_HOST / _TOKEN / GENIE_SPACE_ID."""
    try:
        host     = os.environ["DATABRICKS_HOST"]
        token    = os.environ["DATABRICKS_TOKEN"]
        space_id = os.environ["GENIE_SPACE_ID"]
    except KeyError as missing:
        raise SystemExit(f"missing env var: {missing.args[0]}")
    return GenieSpaces(host, token), space_id


def main() -> None:
    """
    CLI: apply a JSON config file (path from argv[1]) to GENIE_SPACE_ID.
    Prints before/after counts.
    """
    import sys
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <path/to/config.json>", file=sys.stderr)
        sys.exit(2)

    client, space_id = _client_from_env()

    with open(sys.argv[1]) as f:
        config = json.load(f)
    config.pop("_comment", None)  # tolerate a leading "_comment" key

    def counts(inner: dict) -> str:
        ins  = inner.get("instructions", {})
        snip = ins.get("sql_snippets", {})
        cfg  = inner.get("config", {})
        return (f"text={len(ins.get('text_instructions', []))}, "
                f"examples={len(ins.get('example_question_sqls', []))}, "
                f"joins={len(ins.get('join_specs', []))}, "
                f"filters={len(snip.get('filters', []))}, "
                f"expressions={len(snip.get('expressions', []))}, "
                f"sample_questions={len(cfg.get('sample_questions', []))}")

    print(f"1. Fetching {space_id!r}...")
    outer, inner, etag = client.get(space_id)
    print(f"   Space: {outer.get('title')!r}")
    print(f"   etag:  {etag[:24]}...")
    print(f"   Before: {counts(inner)}")

    print("\n2. Applying config...")
    new_outer = client.apply_config(space_id, config)
    print(f"   New etag: {new_outer['etag'][:24]}...")
    print(f"   After:  {counts(client.dump(space_id))}")
    print("\nDone. Changes are live — no restart required.")


if __name__ == "__main__":
    main()
