# Python-worker conformance checks

This harness runs SQL through **Calcite JDBC** against the standard
`vgi._test_fixtures.worker`. It reads the VGI project's SQLLogicTest files and
compares results or expected errors. DuckDB is installed because the Python
fixture uses it to evaluate pushed predicates; Calcite plans and executes the
client query.

## Run the checks

Use JDK 21 and Python 3.14 on macOS or Linux:

```sh
./mvnw clean verify
python3.14 -m venv .conformance/venv
.conformance/venv/bin/python -m pip install -r conformance/requirements.txt
.conformance/venv/bin/python -m unittest discover -s conformance -v

git clone --branch v2.0 https://github.com/Query-farm/vgi.git .conformance/upstream
git -C .conformance/upstream checkout e75482339d07244105e4a040f998a7092b7402a5

.conformance/venv/bin/python conformance/run.py \
  --suite conformance/sql --report target/conformance/adapter/report.json

.conformance/venv/bin/python conformance/run.py \
  --suite .conformance/upstream/test/sql \
  --include integration/table/double_sequence.test \
  --include integration/table/positional_args.test \
  --include integration/table/projection_info.test \
  --report target/conformance/upstream/report.json
```

Add `--transport http` to repeat either run over HTTP. CI runs both transports,
uploads the reports and logs, and fails on any failure or unsupported record in
the selected files. The upstream gate currently selects **100 records**; the
adapter-specific suite adds **14 separate records**. Neither number represents
coverage of the whole upstream suite.

CI also runs **8 malformed-worker checks** per transport using the upstream
fixture that corrupts Arrow buffers after constructing its batches:

```sh
.conformance/venv/bin/python conformance/run.py \
  --suite conformance/malformed \
  --worker-script .conformance/upstream/test/support/malformed_batch_worker.py \
  --report target/conformance/malformed/report.json
```

These checks require rejection of invalid string/list offsets, invalid UTF-8,
and invalid dictionary indices. A `COUNT(*)` case checks validation when the
query does not decode any returned column values.

For an existing VGI checkout, pass its `test/sql` directory to `--suite`.
For a source worker, use `--worker-python /path/to/its/python` and
`--worker-source /path/to/vgi-python`. Its matching RPC dependency must already
be installed in that interpreter. The harness does not repair or modify either
checkout.

To investigate a broader subset:

```sh
.conformance/venv/bin/python conformance/run.py \
  --suite ../vgi/test/sql --include 'integration/table/*.test' \
  --report target/conformance/table-survey/report.json
```

Broader runs currently fail. Keep those failures in the report while fixing the
connector or identifying engine-specific expectations. Omit `--include` to
attempt every file, or use `--inventory-only` to count records without launching
a worker. The inventory covers `.test` files, not `.test_slow` files or expanded
loop iterations.

## What the report means

Each static statement/query record has its source path, line and status:

- `pass`: Calcite returned the expected result or the expected matching error.
- `fail`: execution, result comparison, error matching or the harness failed.
- `unsupported`: the harness cannot faithfully implement a required directive,
  setup option or separate worker fixture.
- `not_run`: the file was not selected, or an earlier setup/bridge failure
  prevented its execution.

Every file remains in the inventory, including files outside `--include`.
Reports contain source hashes, Git revisions, worker package versions and
module path, Calcite connection properties, translated SQL, and failure
details. `worker.log` and `calcite.log` sit beside each report. A timeout or
connection failure is a failure; it is never treated as a passing query.

## Translation and limits

The harness translates a basic VGI `ATTACH` into a Calcite schema, wraps producer
function calls in `TABLE(...)`, supplies the fixture's `main` schema for
two-part function names, and translates named arguments to Calcite's `=>`
syntax. Mixed positional/named calls use advertised parameter names. It does
not insert missing argument defaults or replace worker functions with local
implementations.

Identifiers are quoted so fixture columns such as `value` do not collide with
Calcite keywords. The SQLGlot generator preserves one-based SQL array indices
instead of emitting the neutral AST's zero-based subscripts.

Calcite's `BABEL` conformance mode accepts `%` and grouping by select-list
aliases. Other SQL is parsed and rendered by SQLGlot. DuckDB-specific functions,
settings, joins, metadata tables, numeric semantics and plan text are not
emulated. The report preserves both source and translated SQL so a failure can
be reviewed against that boundary.

This is a bounded SQLLogicTest reader. `require`, `require-env` and
`hash-threshold` declarations do not execute SQL. Unsupported controls such as
loops and restarts remain visible as `unsupported`; they are not silently
ignored. Unknown `ATTACH` options and alternate workers are also rejected by
the harness. Query comparisons support row/value sorting, hashes, NULL text,
and absolute tolerance of `1e-9` for non-hashed `R` cells. Expected error text
must match; merely receiving any error does not pass a negative test.

See [production readiness](../docs/production-readiness.md) for the broader
qualification status.
