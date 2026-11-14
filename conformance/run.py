#!/usr/bin/env python3
"""Execute upstream SQLLogicTests through Calcite; retain all unexecuted cases in the inventory."""

from __future__ import annotations

import argparse
import collections
import contextlib
import dataclasses
import datetime
import decimal
import hashlib
import json
import os
from pathlib import Path
import re
import select
import subprocess
import sys
import time

import sqlglot
from sqlglot import Dialect, exp


class CalciteSql(Dialect):
    # SQLGlot's neutral AST normalizes constant array subscripts to zero-based;
    # Calcite's SQL array access, like DuckDB's, is one-based.
    INDEX_OFFSET = 1


@dataclasses.dataclass
class Record:
    path: str
    line: int
    header: str
    sql: str
    expected: list[str]
    controls: list[str]

    @property
    def key(self):
        return f"{self.path}:{self.line}"


class UnsupportedSyntax(ValueError):
    """An upstream setup operation that the harness cannot translate faithfully."""


def attachment(sql: str) -> tuple[str, str] | None:
    if not re.match(r"\s*ATTACH\b", sql, re.I):
        return None
    match = re.fullmatch(r"\s*ATTACH\s+'([^']+)'(?:\s+AS\s+(\w+))?\s*\((.*)\)\s*;?\s*", sql, re.I | re.S)
    if not match:
        raise UnsupportedSyntax("ATTACH syntax is outside the harness's supported subset")
    options = [part.strip() for part in match[3].split(",")]
    # Never discard unrecognized options: doing so could turn a negative test
    # into a false pass, or query the wrong worker/version/schema.
    if len(options) != 2 or sum(bool(re.fullmatch(r"TYPE\s+vgi", p, re.I)) for p in options) != 1 or sum(
            bool(re.fullmatch(r"LOCATION\s+'\$\{VGI_TEST_WORKER\}'", p, re.I)) for p in options) != 1:
        raise UnsupportedSyntax("ATTACH requires options or a worker fixture not implemented by this harness")
    return match[1], match[2] or match[1]


def records(path: Path, root: Path) -> list[Record]:
    lines = path.read_text().splitlines()
    result = []
    controls = []
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        if not line or line.startswith("#"):
            index += 1
            continue
        if not line.startswith(("query ", "statement ")):
            controls.append(line)
            index += 1
            continue
        start, header = index + 1, line
        index += 1
        sql, expected = [], []
        while index < len(lines) and lines[index].strip() and lines[index] != "----":
            if not lines[index].lstrip().startswith("#"):
                sql.append(lines[index])
            index += 1
        if index < len(lines) and lines[index] == "----":
            index += 1
            while index < len(lines) and lines[index].strip():
                expected.append(lines[index])
                index += 1
        result.append(Record(str(path.relative_to(root)), start, header, "\n".join(sql), expected, list(controls)))
    return result


def normalize_sql(sql: str, aliases: set[str], parameters: dict[str, list[str]] | None = None) -> str:
    """Adapt syntax only: TABLE wrappers, default main schemas, and named argument spelling."""
    tree = sqlglot.parse_one(sql, read="duckdb")
    for table in list(tree.find_all(exp.Table)):
        function = table.this
        if not isinstance(function, exp.Anonymous) or table.db not in aliases and table.catalog not in aliases:
            continue
        arguments = []
        for arg in function.expressions:
            if isinstance(arg, (exp.PropertyEQ, exp.Kwarg)):
                arg = exp.Kwarg(this=exp.to_identifier(arg.this.name, quoted=True), expression=arg.expression.copy())
            elif isinstance(arg, exp.EQ) and isinstance(arg.this, exp.Column) and not arg.this.table:
                arg = exp.Kwarg(this=exp.to_identifier(arg.this.name, quoted=True), expression=arg.expression.copy())
            arguments.append(arg)
        function.set("expressions", arguments)
        prefix = [table.catalog, table.db] if table.catalog else [table.db, "main"]
        key = ".".join([*prefix, function.name]).lower()
        if any(isinstance(arg, exp.Kwarg) for arg in arguments):
            names = (parameters or {}).get(key, [])
            arguments = [
                arg if isinstance(arg, exp.Kwarg) else
                exp.Kwarg(this=exp.to_identifier(names[index], quoted=True), expression=arg)
                if index < len(names) else arg
                for index, arg in enumerate(arguments)
            ]
            function.set("expressions", arguments)
        qualified = exp.Dot.build([*[exp.to_identifier(p) for p in prefix], function.copy()])
        table.set("this", exp.Anonymous(this="TABLE", expressions=[qualified]))
        table.set("catalog", None)
        table.set("db", None)
    for dot in list(tree.find_all(exp.Dot)):
        if isinstance(dot.this, exp.Identifier) and dot.this.name in aliases and isinstance(dot.expression, exp.Anonymous):
            dot.set("this", exp.Dot(this=dot.this.copy(), expression=exp.to_identifier("main")))
    # Names such as value/range are legal fixture columns but reserved words in
    # Calcite. Quote identifiers without changing the expressions they name.
    return tree.sql(dialect=CalciteSql, identify=True)


class Bridge:
    def __init__(self, repo: Path, java: str, log):
        self.process = subprocess.Popen(
            [java, "--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED",
             "-Dio.netty.noUnsafe=false", "-cp", os.pathsep.join([str(repo / "target/test-classes"),
             str(repo / "target/classes"), str(repo / "target/dependency/*")]), "farm.query.vgicalcite.SqlLogicBridge"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True, bufsize=1)

    def request(self, request: dict, timeout: float):
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        ready, _, _ = select.select([self.process.stdout], [], [], timeout)
        if not ready:
            raise TimeoutError(f"Calcite did not complete within {timeout} seconds")
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError(f"Calcite bridge exited: {self.process.poll()}")
        return json.loads(line)

    def close(self):
        if self.process.stdin:
            with contextlib.suppress(BrokenPipeError):
                self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


def compare(record: Record, response: dict) -> tuple[bool, str]:
    header = record.header.split()
    if header[0] == "statement":
        wants_error = header[1] == "error"
        if wants_error:
            if response["ok"]:
                return False, "Expected statement to fail"
            expected = "\n".join(record.expected) or " ".join(header[2:])
            error = response.get("error", "")
            if expected.startswith("<REGEX>:"):
                return bool(re.search(expected[len("<REGEX>:"):].strip(), error, re.S)), "Error regex differs"
            return not expected or expected.casefold() in error.casefold(), "Error text differs"
        return response["ok"], response.get("error", "")
    if not response["ok"]:
        return False, response.get("error", "")
    width = len(header[1])
    rows = response.get("rows", [])
    if len(response.get("columns", [])) != width:
        return False, f"Expected {width} columns, got {len(response.get('columns', []))}"
    order = header[2] if len(header) > 2 else "nosort"
    # DuckDB's SQLLogicTest format represents empty strings and embedded NULs
    # before sorting or hashing. Keep NULL distinct from an empty string.
    actual = [[cell.replace("\x00", r"\0") if cell else "(empty)" for cell in row] for row in rows]
    if order == "rowsort":
        actual.sort()
    values = [cell for row in actual for cell in row]
    if order == "valuesort":
        values.sort()
    if len(record.expected) == 1 and (match := re.fullmatch(r"(\d+) values hashing to ([0-9a-f]+)", record.expected[0])):
        digest = hashlib.md5("".join(v + "\n" for v in values).encode()).hexdigest()
        return len(values) == int(match[1]) and digest == match[2], "Result hash differs"
    expected_rows = [line.split("\t") for line in record.expected]
    if all(len(row) == width for row in expected_rows):
        if order == "rowsort":
            expected_rows.sort()
        expected = [cell for row in expected_rows for cell in row]
    else:
        expected = record.expected
    if order == "valuesort":
        expected = sorted(expected)
    if len(values) != len(expected):
        return False, f"Expected {len(expected)} values, got {len(values)}"
    for index, (want, got) in enumerate(zip(expected, values)):
        if want == got:
            continue
        if header[1][index % width] == "R":
            try:
                if abs(decimal.Decimal(want) - decimal.Decimal(got)) <= decimal.Decimal("0.000000001"):
                    continue
            except decimal.InvalidOperation:
                pass
        return False, f"Value {index}: expected {want[:256]!r}, got {got[:256]!r} (up to 256 characters shown)"
    return True, ""


def git_revision(path: Path):
    result = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"], text=True, capture_output=True)
    return result.stdout.strip() if result.returncode == 0 else None


def source_hashes(repo: Path):
    paths = sorted([repo / "pom.xml", *repo.glob("src/**/*.java"), *repo.glob("conformance/*.py")])
    return {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def worker_metadata(python: str, environment: dict):
    script = """import importlib.metadata, json, sys, vgi
print(json.dumps({'python': sys.version, 'module': vgi.__file__, 'packages': {
    n: importlib.metadata.version(n) for n in ['vgi-python', 'vgi-rpc', 'pyarrow']}}))
"""
    result = subprocess.run([python, "-c", script], env=environment, text=True, capture_output=True, timeout=30)
    if result.returncode:
        raise RuntimeError("Worker environment cannot import VGI:\n" + result.stderr)
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, required=True, help="Upstream vgi/test/sql directory")
    parser.add_argument("--include", action="append", default=[], help="Relative glob; others remain not_run in the report")
    parser.add_argument("--report", type=Path, default=Path("target/conformance/report.json"))
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--worker-python", default=sys.executable)
    parser.add_argument("--worker-source", type=Path, help="Optional Python checkout to prepend to the worker's PYTHONPATH")
    parser.add_argument("--worker-script", type=Path, help="Standalone fixture script instead of the standard worker module")
    parser.add_argument("--java", default=os.environ.get("JAVA_HOME", "") + "/bin/java" if os.environ.get("JAVA_HOME") else "java")
    parser.add_argument("--transport", choices=["tcp", "http"], default="tcp")
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    args.suite = args.suite.resolve()
    args.report = args.report.resolve()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(args.suite.rglob("*.test"))
    if not files:
        parser.error("The suite directory contains no .test files")
    corpus = {str(path.relative_to(args.suite)): records(path, args.suite) for path in files}
    selected = [name for name in corpus if not args.include or any(Path(name).match(p) for p in args.include)]
    if not selected:
        parser.error("No suite files match --include")
    outcomes = {record.key: {"status": "not_run", "path": record.path, "line": record.line, "header": record.header}
                for group in corpus.values() for record in group}
    report = {"started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "suite_root": str(args.suite), "suite_revision": git_revision(args.suite),
              "connector_revision": git_revision(repo), "transport": args.transport,
              "connector_source_sha256": source_hashes(repo),
              "runtime_artifacts_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                           for p in sorted((repo / "target/dependency").glob("*.jar"))},
              "calcite_properties": {"caseSensitive": False, "timeZone": "UTC", "conformance": "BABEL",
                                     "typeSystem": "farm.query.vgicalcite.VgiTypeSystem"},
              "selected_files": selected, "sqlglot_version": sqlglot.__version__,
              "worker_source": str(args.worker_source) if args.worker_source else "installed distribution",
              "file_sha256": {str(p.relative_to(args.suite)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
              "results": outcomes}

    def save():
        report["counts"] = dict(collections.Counter(item["status"] for item in outcomes.values()))
        args.report.write_text(json.dumps(report, indent=2) + "\n")

    save()
    print(f"Inventoried {len(files)} files and {len(outcomes)} records", flush=True)
    if args.inventory_only:
        return 0
    command = [args.worker_python, "-m", "vgi._test_fixtures.worker"]
    if args.worker_script:
        script = args.worker_script.resolve()
        command = [args.worker_python, str(script)]
        report["worker_script"] = {"path": str(script), "sha256": hashlib.sha256(script.read_bytes()).hexdigest()}
    command += ["--tcp", "127.0.0.1:0"] if args.transport == "tcp" else ["--http", "--host", "127.0.0.1", "--port", "0"]
    environment = dict(os.environ, OPENBLAS_NUM_THREADS="1")
    if args.worker_source:
        environment["PYTHONPATH"] = str(args.worker_source.resolve()) + os.pathsep + environment.get("PYTHONPATH", "")
        report["worker_source_revision"] = git_revision(args.worker_source)
    report["worker"] = worker_metadata(args.worker_python, environment)
    with (args.report.parent / "worker.log").open("w") as worker_log, (args.report.parent / "calcite.log").open("w") as java_log:
        worker = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=worker_log, text=True, env=environment)
        try:
            ready, _, _ = select.select([worker.stdout], [], [], 30)
            if not ready:
                raise TimeoutError("Python worker startup timed out")
            banner = worker.stdout.readline().strip()
            if args.transport == "tcp" and banner.startswith("TCP:"):
                location = "tcp://" + banner[4:]
            elif args.transport == "http" and banner.startswith("PORT:"):
                location = "http://127.0.0.1:" + banner[5:]
            else:
                raise RuntimeError(f"Python worker startup failed: {banner!r}; see worker.log")
            report["worker_command"] = command
            for name in selected:
                bridge = Bridge(repo, args.java, java_log)
                aliases = set()
                parameters = {}
                fatal = None
                try:
                    for record in corpus[name]:
                        entry = outcomes[record.key]
                        entry["sql"] = record.sql
                        if fatal:
                            entry.update(status="not_run", reason=f"Earlier bridge failure: {fatal}")
                            continue
                        unsupported_controls = [c for c in record.controls if c.split()[0] not in ("require", "require-env", "hash-threshold")]
                        if unsupported_controls:
                            entry.update(status="unsupported", reason="Harness control: " + ", ".join(unsupported_controls))
                            continue
                        try:
                            attach = attachment(record.sql)
                            detach = re.fullmatch(r"DETACH\s+(\w+)\s*;?", record.sql, re.I)
                            if attach:
                                catalog, alias = attach
                                response = bridge.request({"op": "attach", "alias": alias,
                                                           "operand": {"catalog": catalog, "location": location}}, args.timeout)
                                if response["ok"]:
                                    aliases.add(alias)
                                    parameters.update(response.get("function_parameters", {}))
                                if "unsupported_functions" in response:
                                    report.setdefault("unsupported_functions", {})[catalog] = response["unsupported_functions"]
                            elif detach:
                                response = bridge.request({"op": "detach", "alias": detach.group(1)}, args.timeout)
                                if response["ok"]:
                                    aliases.discard(detach.group(1))
                            else:
                                sql = normalize_sql(record.sql, aliases, parameters)
                                entry["calcite_sql"] = sql
                                response = bridge.request({"sql": sql, "timeout": int(args.timeout)}, args.timeout)
                            matched, detail = compare(record, response)
                            entry.update(status="pass" if matched else "fail", elapsed_ms=response["elapsed_ms"])
                            if not matched:
                                preview = [[cell if len(cell) <= 512 else cell[:512] + "... (truncated)" for cell in row]
                                           for row in response.get("rows", [])[:20]]
                                entry.update(reason=detail, error=response.get("error"), actual=preview,
                                             expected=record.expected[:20])
                                if attach:
                                    fatal = "Required ATTACH did not meet its expectation"
                        except UnsupportedSyntax as failure:
                            entry.update(status="unsupported", reason=str(failure))
                            fatal = str(failure)
                        except (TimeoutError, BrokenPipeError, RuntimeError) as failure:
                            fatal = str(failure)
                            entry.update(status="fail", reason=fatal)
                        except Exception as failure:
                            entry.update(status="fail", reason=f"{type(failure).__name__}: {failure}")
                finally:
                    bridge.close()
                counts = collections.Counter(outcomes[r.key]["status"] for r in corpus[name])
                print(name + ": " + json.dumps(dict(counts)), flush=True)
                save()
        finally:
            worker.terminate()
            try:
                worker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)
            save()
    print(json.dumps(report["counts"]), flush=True)
    return int(report["counts"].get("fail", 0) > 0 or report["counts"].get("unsupported", 0) > 0)


if __name__ == "__main__":
    raise SystemExit(main())
