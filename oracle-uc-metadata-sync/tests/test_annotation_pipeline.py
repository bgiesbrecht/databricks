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

UC = "cat.sch.po_edd_mv"

# ── fixtures straight from Ben's 2026-07-10 email ─────────────────────────────
FK_JSON = ('{ "left_table": "po_edd_mv", "right_table": "all_users_v1_mv", "join_condition": "=", '
           '"left_column": "per_intr_no_buy", "right_column": "per_intr_no", '
           '"relationship": "Many to One", "Instructions": "", "Type": "Join" }')

# Invalid JSON on purpose: unescaped double quotes inside "instructions".
SQL_FILTER_JSON_BAD = ('{ "name": "Contracts", "code": "PO_APPL_DESC in (\'LINCS Subcontract\', \'PARIS PO\')", '
                       '"synonyms": "Subcontracts, Contracts", '
                       '"instructions": "When the user searches for "Contracts" or "Subcontracts" apply this filter.", '
                       '"Type": "Filter" }')

SQL_MEASURE_JSON = ('{ "name": "Total Spend", "code": "SUM(PO_DOL_GRS_AMT)", "synonyms": "Gross Spend", '
                    '"instructions": "use for spend", "Type": "Expression" }')

SAMPLE_QUERY_JSON = ('{ "name": "Orders for Project Task", '
                     '"question": "Which orders are tied to P/T 123456?", '
                     '"query": "select distinct po.po_no from lincsvectr.po_edd_mv po where pt.project_no = :project" }')


def row(**kw):
    base = dict(sync_name="s", oracle_schema="LINCSVECTR", oracle_object="PO_EDD_MV",
                oracle_column=None, level="TABLE", object_type="TABLE",
                annotation_name=None, annotation_value=None, uc_name=UC, is_active="true")
    base.update(kw)
    return base


# ── tolerant JSON ─────────────────────────────────────────────────────────────
class TestTolerantJson(unittest.TestCase):
    def test_valid_json_parses_strict_no_note(self):
        d, note = parse_json_tolerant(FK_JSON)
        self.assertIsNone(note)
        self.assertEqual(d["left_table"], "po_edd_mv")

    def test_malformed_json_repaired_with_note(self):
        d, note = parse_json_tolerant(SQL_FILTER_JSON_BAD)
        self.assertIsNotNone(note)
        self.assertEqual(d["name"], "Contracts")
        self.assertEqual(d["code"], "PO_APPL_DESC in ('LINCS Subcontract', 'PARIS PO')")
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
        self.assertEqual((fk.left_column, fk.right_table, fk.right_column), ("per_intr_no_buy", "all_users_v1_mv", "per_intr_no"))
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
        self.assertEqual(se.code, "SUM(PO_DOL_GRS_AMT)")

    def test_sql_expression_missing_code_errors(self):
        se, err = parse_sql_expression("sql_expression_x", '{ "name": "X", "Type": "Filter" }')
        self.assertIsNone(se)
        self.assertIsNotNone(err)

    def test_sample_query_fields(self):
        sq, err = parse_sample_query("sample_query_ord_for_pt", SAMPLE_QUERY_JSON)
        self.assertIsNone(err)
        self.assertEqual(sq.label, "ord_for_pt")
        self.assertEqual(sq.question, "Which orders are tied to P/T 123456?")
        self.assertTrue(sq.query.startswith("select distinct"))


# ── row classification ────────────────────────────────────────────────────────
class TestParseRows(unittest.TestCase):
    def test_foreign_key_row(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="PER_INTR_NO_BUY",
                            level="COLUMN", annotation_value=FK_JSON)])
        self.assertEqual(len(p.objects["LINCSVECTR.PO_EDD_MV"].foreign_keys), 1)

    def test_sql_expression_and_sample_query_rows(self):
        p = parse_rows([
            row(annotation_name="sql_expression_contracts", annotation_value=SQL_FILTER_JSON_BAD),
            row(annotation_name="sample_query_ord_for_pt", annotation_value=SAMPLE_QUERY_JSON),
        ])
        oa = p.objects["LINCSVECTR.PO_EDD_MV"]
        self.assertEqual(len(oa.sql_expressions), 1)
        self.assertEqual(len(oa.sample_queries), 1)

    def test_inactive_row_skipped(self):
        p = parse_rows([row(annotation_name="foreign_key", annotation_value=FK_JSON, is_active="false")])
        self.assertEqual(len(p.objects), 0)

    def test_bad_json_missing_fields_flagged_not_dropped(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="C", level="COLUMN",
                            annotation_value='{ "left_table": "a" }')])
        oa = p.objects["LINCSVECTR.PO_EDD_MV"]
        self.assertEqual(len(oa.foreign_keys), 0)
        self.assertEqual(len(oa.unknown), 1)

    def test_repaired_surfaced(self):
        p = parse_rows([row(annotation_name="sql_expression_contracts", annotation_value=SQL_FILTER_JSON_BAD)])
        self.assertEqual(len(p.repaired()), 1)


# ── renderer helpers ──────────────────────────────────────────────────────────
class TestRenderHelpers(unittest.TestCase):
    def test_guess_uc_reuses_catalog_schema(self):
        self.assertEqual(_guess_uc("cat.sch.po_edd_mv", "ALL_USERS_V1_MV"), "cat.sch.all_users_v1_mv")

    def test_unique_alias_suffixes_on_collision(self):
        used = {"po_edd_mv"}
        self.assertEqual(_unique_alias("all_users_v1_mv", used), "all_users_v1_mv")
        self.assertEqual(_unique_alias("all_users_v1_mv", used), "all_users_v1_mv_2")

    def test_translate_oracle_sql_rewrites_schema_and_finds_binds(self):
        sql = ("select * from lincsvectr.po_edd_mv po "
               "join LINCSVECTR.po_ln_mv pol on pol.po_no = po.po_no where pt.project_no = :project")
        out, binds = translate_oracle_sql(sql, "cat", "sch", "lincsvectr")
        self.assertIn("cat.sch.po_edd_mv", out)
        self.assertIn("cat.sch.po_ln_mv", out)           # case-insensitive prefix match
        self.assertNotIn("lincsvectr.", out.lower())
        self.assertEqual(binds, ["project"])

    def test_translate_leaves_table_aliases_untouched(self):
        out, _ = translate_oracle_sql("select po.po_no from lincsvectr.po_edd_mv po", "c", "s", "lincsvectr")
        self.assertIn("po.po_no", out)                   # alias, not schema-qualified, unchanged


# ── end-to-end render ─────────────────────────────────────────────────────────
class TestRenderGenieConfig(unittest.TestCase):
    def test_join_uses_json_relationship_and_condition(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="PER_INTR_NO_BUY",
                            level="COLUMN", annotation_value=FK_JSON)])
        cfg, rep = render_genie_config(p)
        j = cfg["joins"][0]
        self.assertEqual(j["relationship_type"], "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE")
        self.assertEqual(j["on"], "`po_edd_mv`.`per_intr_no_buy` = `all_users_v1_mv`.`per_intr_no`")
        self.assertEqual(rep.joins, 1)

    def test_role_playing_dim_distinct_aliases(self):
        fk2 = FK_JSON.replace("per_intr_no_buy", "per_intr_no_rqst_by")
        p = parse_rows([
            row(annotation_name="foreign_key", oracle_column="PER_INTR_NO_BUY", level="COLUMN", annotation_value=FK_JSON),
            row(annotation_name="foreign_key", oracle_column="PER_INTR_NO_RQST_BY", level="COLUMN", annotation_value=fk2),
        ])
        cfg, _ = render_genie_config(p)
        self.assertEqual([j["right"]["alias"] for j in cfg["joins"]], ["all_users_v1_mv", "all_users_v1_mv_2"])

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
        self.assertIn("cat.sch.po_edd_mv", sql)          # lincsvectr. rewritten to UC name
        self.assertNotIn("lincsvectr.", sql.lower())
        self.assertEqual(len(rep.warnings), 1)           # :project bind var remains -> flagged

    def test_resolver_overrides_heuristic_for_join_targets(self):
        p = parse_rows([row(annotation_name="foreign_key", oracle_column="PER_INTR_NO_BUY",
                            level="COLUMN", annotation_value=FK_JSON)])
        # authoritative resolver sends the right table to a different catalog/schema than the heuristic
        resolve = {"PO_EDD_MV": "prod.finance.po_edd_mv",
                   "ALL_USERS_V1_MV": "prod.reference.all_users_v1_mv"}.get
        cfg, _ = render_genie_config(p, resolve=lambda o: resolve(o.upper()))
        j = cfg["joins"][0]
        self.assertEqual(j["left"]["table"], "prod.finance.po_edd_mv")
        self.assertEqual(j["right"]["table"], "prod.reference.all_users_v1_mv")

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
