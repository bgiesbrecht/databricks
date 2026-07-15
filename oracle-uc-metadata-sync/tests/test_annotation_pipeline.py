"""
Unit tests for annotation_parser + annotation_to_genie (JSON annotation format).

Run:  python -m unittest discover -s tests -v      (from the repo root)
   or python tests/test_annotation_pipeline.py

No external deps — stdlib unittest only. Adds ../src to sys.path.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from annotation_parser import (  # noqa: E402
    parse_rows, parse_json_tolerant, parse_foreign_key, parse_sql_expression, parse_sample_query,
)
from annotation_to_genie import (  # noqa: E402
    render_genie_config, translate_oracle_sql, _unique_alias, _guess_uc,
)

UC = "cat.sch.orders"

# ── fixtures from the 2026-07 annotation JSON spec ────────────────────────────
FK_JSON = ('{ "left_table": "orders", "right_table": "customers", "join_condition": "=", '
           '"left_column": "buyer_id", "right_column": "customer_id", '
           '"relationship": "Many to One", "Instructions": "", "Type": "Join" }')

# Invalid JSON on purpose: unescaped double quotes inside "instructions".
SQL_FILTER_JSON_BAD = ('{ "name": "Contracts", "code": "ORDER_TYPE in (\'Wholesale\', \'Distributor\')", '
                       '"synonyms": "Subcontracts, Contracts", '
                       '"instructions": "When the user searches for "Contracts" or "Subcontracts" apply this filter.", '
                       '"Type": "Filter" }')

SQL_MEASURE_JSON = ('{ "name": "Total Spend", "code": "SUM(GROSS_AMOUNT)", "synonyms": "Gross Spend", '
                    '"instructions": "use for spend", "Type": "Expression" }')

SAMPLE_QUERY_JSON = ('{ "name": "Orders for Project Task", '
                     '"question": "Which orders are tied to project task 12345?", '
                     '"query": "select distinct po.order_id from sales.orders po where pt.project_no = :project" }')


def row(**kw):
    base = dict(sync_name="s", oracle_schema="SALES", oracle_object="ORDERS",
                oracle_column=None, level="TABLE", object_type="TABLE",
                annotation_name=None, annotation_value=None, uc_name=UC, is_active="true")
    base.update(kw)
    return base


# ── tolerant JSON ─────────────────────────────────────────────────────────────
class TestTolerantJson(unittest.TestCase):
    def test_valid_json_parses_strict_no_note(self):
        d, note = parse_json_tolerant(FK_JSON)
        self.assertIsNone(note)
        self.assertEqual(d["left_table"], "orders")

    def test_malformed_json_repaired_with_note(self):
        d, note = parse_json_tolerant(SQL_FILTER_JSON_BAD)
        self.assertIsNotNone(note)
        self.assertEqual(d["name"], "Contracts")
        self.assertEqual(d["code"], "ORDER_TYPE in ('Wholesale', 'Distributor')")
        self.assertEqual(d["Type"], "Filter")
        # the inner quotes are preserved in the recovered value
        self.assertIn('"Contracts"', d["instructions"])

    def test_non_json_reports_error(self):
        d, note = parse_json_tolerant("not json at all")
        self.assertEqual(d, {})
        self.assertIsNotNone(note)


# ── value parsers ─────────────────────────────────────────────────────────────
class TestValueParsers(unittest.TestCase):
    def test_foreign_key_fields_and_relationship_norm(self):
        fk, err = parse_foreign_key(FK_JSON)
        self.assertIsNone(err)
        self.assertEqual((fk.left_column, fk.right_table, fk.right_column), ("buyer_id", "customers", "customer_id"))
        self.assertEqual(fk.relationship, "MANY_TO_ONE")
        self.assertEqual(fk.join_condition, "=")

    def test_foreign_key_missing_field_errors(self):
        fk, err = parse_foreign_key('{ "left_table": "a", "left_column": "b" }')
        self.assertIsNone(fk)
        self.assertIsNotNone(err)

    def test_sql_expression_filter_kind_and_synonyms(self):
        se, err = parse_sql_expression("sql_expression_contracts", SQL_FILTER_JSON_BAD)
        self.assertIsNone(err)
        self.assertEqual(se.label, "contracts")
        self.assertEqual(se.kind, "filter")
        self.assertEqual(se.synonyms, ["Subcontracts", "Contracts"])
        self.assertIsNotNone(se.parse_note)  # came through the lenient path

    def test_sql_expression_measure_is_expression_kind(self):
        se, _ = parse_sql_expression("sql_expression_total_spend", SQL_MEASURE_JSON)
        self.assertEqual(se.kind, "expression")
        self.assertEqual(se.code, "SUM(GROSS_AMOUNT)")

    def test_sql_expression_missing_code_errors(self):
        se, err = parse_sql_expression("sql_expression_x", '{ "name": "X", "Type": "Filter" }')
        self.assertIsNone(se)
        self.assertIsNotNone(err)

    def test_sample_query_fields(self):
        sq, err = parse_sample_query("sample_query_ord_for_pt", SAMPLE_QUERY_JSON)
        self.assertIsNone(err)
        self.assertEqual(sq.label, "ord_for_pt")
        self.assertEqual(sq.question, "Which orders are tied to project task 12345?")
        self.assertTrue(sq.query.startswith("select distinct"))


# ── row classification ────────────────────────────────────────────────────────
class TestParseRows(unittest.TestCase):
    def test_foreign_key_row(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="BUYER_ID",
                            level="COLUMN", annotation_value=FK_JSON)])
        self.assertEqual(len(p.objects["SALES.ORDERS"].foreign_keys), 1)

    def test_sql_expression_and_sample_query_rows(self):
        p = parse_rows([
            row(annotation_name="sql_expression_contracts", annotation_value=SQL_FILTER_JSON_BAD),
            row(annotation_name="sample_query_ord_for_pt", annotation_value=SAMPLE_QUERY_JSON),
        ])
        oa = p.objects["SALES.ORDERS"]
        self.assertEqual(len(oa.sql_expressions), 1)
        self.assertEqual(len(oa.sample_queries), 1)

    def test_inactive_row_skipped(self):
        p = parse_rows([row(annotation_name="foreign_key", annotation_value=FK_JSON, is_active="false")])
        self.assertEqual(len(p.objects), 0)

    def test_bad_json_missing_fields_flagged_not_dropped(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="C", level="COLUMN",
                            annotation_value='{ "left_table": "a" }')])
        oa = p.objects["SALES.ORDERS"]
        self.assertEqual(len(oa.foreign_keys), 0)
        self.assertEqual(len(oa.unknown), 1)

    def test_repaired_surfaced(self):
        p = parse_rows([row(annotation_name="sql_expression_contracts", annotation_value=SQL_FILTER_JSON_BAD)])
        self.assertEqual(len(p.repaired()), 1)


# ── renderer helpers ──────────────────────────────────────────────────────────
class TestRenderHelpers(unittest.TestCase):
    def test_guess_uc_reuses_catalog_schema(self):
        self.assertEqual(_guess_uc("cat.sch.orders", "CUSTOMERS"), "cat.sch.customers")

    def test_unique_alias_suffixes_on_collision(self):
        used = {"orders"}
        self.assertEqual(_unique_alias("customers", used), "customers")
        self.assertEqual(_unique_alias("customers", used), "customers_2")

    def test_translate_oracle_sql_rewrites_schema_and_finds_binds(self):
        sql = ("select * from sales.orders po "
               "join SALES.order_lines pol on pol.order_id = po.order_id where pt.project_no = :project")
        out, binds = translate_oracle_sql(sql, "cat", "sch", "sales")
        self.assertIn("cat.sch.orders", out)
        self.assertIn("cat.sch.order_lines", out)           # case-insensitive prefix match
        self.assertNotIn("sales.", out.lower())
        self.assertEqual(binds, ["project"])

    def test_translate_leaves_table_aliases_untouched(self):
        out, _ = translate_oracle_sql("select po.order_id from sales.orders po", "c", "s", "sales")
        self.assertIn("po.order_id", out)                   # alias, not schema-qualified, unchanged


# ── end-to-end render ─────────────────────────────────────────────────────────
class TestRenderGenieConfig(unittest.TestCase):
    def test_join_uses_json_relationship_and_condition(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="BUYER_ID",
                            level="COLUMN", annotation_value=FK_JSON)])
        cfg, rep = render_genie_config(p)
        j = cfg["joins"][0]
        self.assertEqual(j["relationship_type"], "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE")
        self.assertEqual(j["on"], "`orders`.`buyer_id` = `customers`.`customer_id`")
        self.assertEqual(rep.joins, 1)

    def test_role_playing_dim_distinct_aliases(self):
        fk2 = FK_JSON.replace("buyer_id", "requested_by_id")
        p = parse_rows([
            row(annotation_name="foreign_key", oracle_column="BUYER_ID", level="COLUMN", annotation_value=FK_JSON),
            row(annotation_name="foreign_key", oracle_column="REQUESTED_BY_ID", level="COLUMN", annotation_value=fk2),
        ])
        cfg, _ = render_genie_config(p)
        self.assertEqual([j["right"]["alias"] for j in cfg["joins"]], ["customers", "customers_2"])

    def test_filter_vs_expression_routing(self):
        p = parse_rows([
            row(annotation_name="sql_expression_contracts", annotation_value=SQL_FILTER_JSON_BAD),
            row(annotation_name="sql_expression_total_spend", annotation_value=SQL_MEASURE_JSON),
        ])
        cfg, rep = render_genie_config(p)
        self.assertEqual(rep.sql_filters, 1)
        self.assertEqual(rep.sql_expressions, 1)
        self.assertEqual(cfg["sql_filters"][0]["synonyms"], ["Subcontracts", "Contracts"])

    def test_sample_query_translated_schema_and_bind_warning(self):
        p = parse_rows([row(annotation_name="sample_query_ord_for_pt", annotation_value=SAMPLE_QUERY_JSON)])
        cfg, rep = render_genie_config(p)
        sql = cfg["examples"][0]["sql"]
        self.assertIn("cat.sch.orders", sql)          # sales. rewritten to UC name
        self.assertNotIn("sales.", sql.lower())
        self.assertEqual(len(rep.warnings), 1)           # :project bind var remains -> flagged

    def test_resolver_overrides_heuristic_for_join_targets(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="BUYER_ID",
                            level="COLUMN", annotation_value=FK_JSON)])
        # authoritative resolver sends the right table to a different catalog/schema than the heuristic
        resolve = {"ORDERS": "prod.finance.orders",
                   "CUSTOMERS": "prod.reference.customers"}.get
        cfg, _ = render_genie_config(p, resolve=lambda o: resolve(o.upper()))
        j = cfg["joins"][0]
        self.assertEqual(j["left"]["table"], "prod.finance.orders")
        self.assertEqual(j["right"]["table"], "prod.reference.customers")

    def test_unresolved_fk_skipped(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="C", level="COLUMN",
                            annotation_value=FK_JSON, uc_name=None)])
        cfg, rep = render_genie_config(p)
        self.assertNotIn("joins", cfg)
        self.assertEqual(len(rep.skipped), 1)

    def test_repaired_surfaced_in_report(self):
        p = parse_rows([row(annotation_name="sql_expression_contracts", annotation_value=SQL_FILTER_JSON_BAD)])
        _, rep = render_genie_config(p)
        self.assertEqual(len(rep.repaired), 1)

    def test_empty_config(self):
        cfg, _ = render_genie_config(parse_rows([]))
        self.assertEqual(cfg, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
