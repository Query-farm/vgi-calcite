# Production readiness

The adapter is a pre-release, read-only connector. Predicate pushdown is
implemented for a bounded subset, but passing most of the upstream VGI suite
has **not** been demonstrated. Production qualification is ongoing.

## Evidence so far

The initial Java fixture suite passed 37 tests. Testing the released standard
Python worker then exposed three defects that those tests missed:

1. Catalog discovery did not offer the read profiles needed for canonical
   constraint metadata. The adapter now negotiates those profiles, validates
   the selected agreement and the SDK's bounded constraint contract, and
   rejects inconsistent metadata. It does not infer optimizer keys or claim
   permission to enforce writes.
2. Single-column table functions returned array rows where Calcite's sort,
   join and union operators expected scalars. The adapter now converts the
   physical rows. Three regression tests reproduced the failure before the
   fix and passed afterward, including nullable and empty results.
3. Canonical catalog constraints can make a column non-nullable even when its
   backing scan function declares it nullable. The adapter now accepts that
   combination and rejects returned NULLs that violate the catalog declaration.

The Java suite now has 56 tests. The Python interoperability gate runs 100
upstream records, 14 adapter-specific records and 8 malformed-worker checks
over TCP and HTTP. The
adapter checks include worker-echoed filters, retained local expressions and
catalog-table reads. See the [conformance instructions](../conformance/README.md)
and [CI workflow](../.github/workflows/ci.yml) for reproducible commands and
report artifacts.

A broader development run against VGI suite revision
`86036b699c533ab87d9ddf44c2b477d7c9675951` inventoried **417 files and 7,211
static records**. It selected seven files: `arg_validation`, `double_sequence`,
`filter_echo`, `joins`, `positional_args`, `sequence`, and
`set_operations_and_subqueries` under `integration/table`.

| Result | Before the row fix and BABEL syntax configuration | After |
| --- | ---: | ---: |
| Passed | 208 | 247 |
| Failed | 78 | 39 |
| Not run | 6,925 | 6,925 |

The worker was released `vgi-python 0.51.1` with `vgi-rpc 0.53.1`, protocol
3.3.0. This is not evidence for an unreleased source checkout with different
dependencies. The CI suite revision is separately pinned to the public commit
`e75482339d07244105e4a040f998a7092b7402a5`; its selected test files match the
development suite above.

The latest **full table-directory survey** ran against source matching connector
commit `2bf3463cdf644acd98ceb2622e696c827f9185e3` and the same development suite and
released Python worker. Its 62 selected files contain 1,480 static records:

| Status | Records |
| --- | ---: |
| Passed | 623 |
| Failed | 822 |
| Unsupported by the harness | 3 |
| Not run after an unavailable setup | 32 |

The remaining 5,731 records outside that directory were not selected. These
counts include DuckDB-specific expectations and harness limitations as well as
connector failures; they are not a count of independent connector bugs. They
also do not support a claim that the connector passes most table tests. The
[recorded baseline](../conformance/baselines/table-survey.json) includes per-file
counts and source hashes for comparison with future runs. This improves on the
previous survey's 609 passes and 836 failures; none of its passing records
regressed. The run began before its source was committed, so the baseline also
records the worktree's base revision and the verified source hashes.

## Function projection and batch validation

Producer table functions now receive column selections as well as supported
filters. Regression tests inspect the worker's requested columns, including
prepared parameters, reordered and repeated outputs, and columns needed only
by residual conditions. Queries with no referenced columns preserve row counts.

The adapter validates decoded batches before reading rows, including count-only
queries. The upstream malformed-batch fixture tests corrupt offsets, invalid
UTF-8 and out-of-range dictionary indices over TCP and HTTP. CI exposed a gap:
the RPC dependency's Arrow loader expands a malformed list's child vector
before adapter validation can see its original length. The list rejection test
currently fails with published RPC versions through 0.33.3.
[SDK PR #3](https://github.com/Query-farm/vgi-rpc-java/pull/3) fixes the reader.
With a locally built copy of that patch, all 122 selected SQL records pass on
both TCP and HTTP. The public CI gate remains failing until a patched SDK is
published and the connector pins it. These local runs record the patched
runtime JAR's hash; they are not results for the published dependency.
Repeating the full table survey with the revised reader produced the same
623 passes and 822 failures, with no record-status changes. The
[SDK qualification record](../conformance/baselines/sdk-reader-validation.json)
identifies the exact SDK source, private artifact and connector source used.
Valid nested lists and structs are also exercised through the Python worker.

Map fields use Arrow's buffer validation without its separate type visitor,
which incorrectly rejects valid maps in Arrow 19. A regression test verifies
both a valid map and rejection of an invalid map offset.

A four-file projection survey passed 134 of 135 records after the harness
quoted reserved column names. Its one remaining failure is
`integration/table/projected_data.test:220`: the expected running sum is
`11,994,000`, but `1.5 * 4000 * 3999 / 2` is `11,997,000`. Calcite and an
independent DuckDB calculation both return the latter. The original upstream
assertion remains a failure in the report; the adapter suite separately checks
the correct result. This file is not part of the all-passing CI gate.

## Remaining qualification work

- **Broader query coverage.** Run the full inventory and distinguish connector
  defects, unimplemented harness controls, and DuckDB-specific expectations.
  The remaining 39 selected failures include named arguments that omit an
  earlier default, integer `AVG` result types, engine-specific SQL and settings,
  and equivalent predicate strings printed differently by the workers.
- **Function coverage.** Scalar, aggregate and table-in/table-out functions
  remain unsupported. They account for substantial parts of the upstream
  suite. Producer functions accept literal scalar arguments only.
- **Pushdown coverage.** Validate more types and expression combinations with
  the standard worker, including required filters and filtered scan planning.
  Join, aggregate, sort and limit pushdown remain unsupported.
- **Operational behavior.** Qualify cancellation during blocked I/O, timeout
  behavior, connection and attachment cleanup under failures, concurrent
  queries, memory bounds and long-running scans. Each scan currently attaches
  independently; there is no connection pool.
- **Catalog behavior.** Qualify schema changes, version consistency and broader
  catalog metadata. Discovery is a snapshot. Constraint CHECK expressions use
  the pinned SDK's restricted numeric grammar and evaluation context.
- **Release criteria.** Publish a scoped compatibility matrix, reproducible
  conformance reports and performance measurements before describing this
  connector as production-ready. Writes and transactions would require a
  separately implemented and tested contract.

An unrun record is not a pass. A passing selected subset does not establish
compatibility with most of the full VGI suite.
