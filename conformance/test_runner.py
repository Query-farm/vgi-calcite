import hashlib
from pathlib import Path
import tempfile
import unittest

from run import Record, UnsupportedSyntax, attachment, compare, normalize_sql, records


class RunnerTest(unittest.TestCase):
    def test_inventory_retains_expected_errors_controls_and_line_numbers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "example.test"
            path.write_text("# comment\nrequire vgi\n\nstatement error\nSELECT missing();\n----\nno match\n\n"
                            "query IT rowsort\nSELECT 1, 'a';\n----\n1\ta\n\nforeach x 1 2\n\n"
                            "query I\nSELECT ${x};\n----\n${x}\n")
            parsed = records(path, root)
            self.assertEqual([r.line for r in parsed], [4, 9, 16])
            self.assertEqual(parsed[0].expected, ["no match"])
            self.assertEqual(parsed[1].expected, ["1\ta"])
            self.assertIn("foreach x 1 2", parsed[2].controls)

    def test_attachment_never_discards_options_or_worker_identity(self):
        self.assertEqual(("example", "alias"), attachment(
            "ATTACH 'example' AS alias (LOCATION '${VGI_TEST_WORKER}', TYPE vgi);"))
        for sql in [
            "ATTACH 'example' (TYPE vgi, LOCATION '${VGI_OTHER_WORKER}');",
            "ATTACH 'example' (TYPE vgi, LOCATION '${VGI_TEST_WORKER}', BAD_OPTION true);",
            "ATTACH 'example';",
        ]:
            with self.assertRaises(UnsupportedSyntax):
                attachment(sql)
        self.assertIsNone(attachment("SELECT 1"))

    def test_sql_translation_preserves_literal_and_null_arguments(self):
        sql = normalize_sql("SELECT * FROM example.sequence(3, increment := NULL)", {"example"},
                            {"example.main.sequence": ["count", "batch_size", "increment"]})
        self.assertIn('TABLE("example"."main".sequence("count" => 3, "increment" => NULL))', sql)
        self.assertNotIn("batch_size", sql)
        sql = normalize_sql("SELECT * FROM example.nested.echo('x := y', 2)", {"example"})
        self.assertIn('TABLE("example"."nested".echo(\'x := y\', 2))', sql)

    def test_sql_translation_quotes_reserved_column_names(self):
        sql = normalize_sql("SELECT value, id FROM example.projected_data(3)", {"example"})
        self.assertTrue(sql.startswith('SELECT "value", "id" FROM TABLE('), sql)

    def test_sql_translation_preserves_array_subscripts(self):
        sql = normalize_sql("SELECT history[1], history[n] FROM example.nested_sequence(3)", {"example"})
        self.assertIn('"history"[1]', sql)
        self.assertIn('"history"["n"]', sql)

    def test_engine_failures_never_become_expected_query_results(self):
        record = Record("a", 1, "query I", "SELECT 1", ["1"], [])
        self.assertFalse(compare(record, {"ok": False, "error": "HTTP connection failure"})[0])
        self.assertFalse(compare(record, {"ok": True, "columns": ["a", "b"], "rows": [["1", "2"]]})[0])

    def test_expected_error_must_match_and_actually_fail(self):
        record = Record("a", 1, "statement error", "bad", ["must be >= 1"], [])
        self.assertFalse(compare(record, {"ok": True})[0])
        self.assertFalse(compare(record, {"ok": False, "error": "unrelated parser failure"})[0])
        self.assertTrue(compare(record, {"ok": False, "error": "value must be >= 1"})[0])

    def test_rowsort_hash_null_and_numeric_comparison(self):
        record = Record("a", 1, "query IR rowsort", "", ["2\tNULL", "1\t1.5"], [])
        response = {"ok": True, "columns": ["i", "r"], "rows": [["1", "1.5000000000"], ["2", "NULL"]]}
        self.assertTrue(compare(record, response)[0])
        response["rows"][0][1] = "1.51"
        self.assertFalse(compare(record, response)[0])
        digest = hashlib.md5(b"1\n2\n").hexdigest()
        record = Record("a", 1, "query I valuesort", "", [f"2 values hashing to {digest}"], [])
        response = {"ok": True, "columns": ["i"], "rows": [["2"], ["1"]]}
        self.assertTrue(compare(record, response)[0])
        response["rows"].append(["3"])
        self.assertFalse(compare(record, response)[0])


if __name__ == "__main__":
    unittest.main()
