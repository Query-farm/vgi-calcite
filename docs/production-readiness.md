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

The Java suite now has 69 tests. The Python interoperability gate runs 100
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

The **full inventory survey** subsequently attempted all 417 files against
connector commit `2b5e5e93d392205f4bbdb7eb30dad76331338335`, using the same
released Python worker over TCP and the private SDK reader build described
below:

| Status | Records |
| --- | ---: |
| Passed | 1,408 |
| Failed | 4,444 |
| Unsupported by the harness | 227 |
| Not run after a setup or bridge failure | 1,132 |

The [full baseline](../conformance/baselines/full-survey.json) records per-file
counts, exact source and dependency hashes, and reproducible classification
rules. Of the failed records, 767 contain engine control statements and 601
reference engine utilities such as DuckDB logs or cache inspection. A further
164 have mismatched expected error text; the remaining 2,912 need individual
review. These are textual observations, not root-cause diagnoses. Unsupported
scalar, aggregate and table-in/table-out functions account for additional
failures, but their directory names alone do not establish each failure's cause.
Passing setup statements also contribute to the pass count. This run predates
the returned-column NULL checks and catalog-version guard below.

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

Every returned top-level column is checked for NULLs against its bind
declaration and any stronger catalog nullability constraint before local
projection. This includes dictionary entries referenced by non-nullable columns.
A SQL regression reproduced an invalid NULL going unchecked when a worker
returned a fallback column that Calcite did not decode; batch validation now
rejects it, while valid nullable data and unused NULL dictionary entries remain
accepted.

A four-file projection survey passed 134 of 135 records after the harness
quoted reserved column names. Its one remaining failure is
`integration/table/projected_data.test:220`: the expected running sum is
`11,994,000`, but `1.5 * 4000 * 3999 / 2` is `11,997,000`. Calcite and an
independent DuckDB calculation both return the latter. The original upstream
assertion remains a failure in the report; the adapter suite separately checks
the correct result. This file is not part of the all-passing CI gate.

## Catalog-version checks

Discovery retains the worker's catalog version. Subsequent function binding
and scans compare their new attachment's version with that snapshot, including
repeated execution of a prepared statement. Known mutable versions are also
checked after metadata discovery, scan planning and binding. A mismatch fails
with an instruction to reconnect, which obtains fresh metadata. Regression
tests reproduce changes during discovery and between prepared executions;
both cases previously proceeded with stale metadata.

Version `0` means unknown and cannot provide this consistency check. These
checks do not provide a transactional snapshot, prevent changes after the last
check, or automatically pin resolved data and implementation versions.

## Cancellation and resource lifetime

Eight SQL regressions exercise explicit JDBC cancellation and one-second query
timeouts over TCP, HTTP, Unix sockets and subprocesses. A marker confirms that
the worker is stalled before the test requests cancellation; the client must
fail within three seconds without releasing the worker. All eight cases failed
before the transport abort path was implemented and pass afterward. The tests
also exercise another statement while a scan is blocked and a fresh query after
cancellation. These are regression bounds, not a production latency guarantee.

The scan monitor closes the underlying connection without waiting for a blocked
RPC response. Arrow cleanup remains on the scan thread. This does not prove
that remote worker computation stops immediately, or qualify cancellation
during metadata discovery and SQL preparation.

A separate repeated-scan regression found that the published RPC SDK retains
client Arrow buffers after successful byte-stream scans: ten completed scans
added 2,560 bytes in the measured fixture. SDK PR #3 now also closes the stream
reader and frees dictionary buffers on failure. Seven SDK resource regressions
pass alongside the 698-test core suite and 21-test FFM suite (three and two
platform-dependent skips, respectively). Cross-platform SDK CI was restarted
for this additional fix; the previous green run predates it.

The connector's SDK release candidate includes the SQL memory regression. With
the private `0.33.4-calcite-v3-SNAPSHOT` build, all 70 Java tests and all 122
selected Python-worker records per transport pass. The release candidate must
pin the published fixed SDK before merging; the main branch still uses 0.33.3.
Production readiness remains blocked on that dependency fix as well as the
broader compatibility and operational work below.

## Remaining qualification work

- **Broader query coverage.** Triage the full inventory's remaining failures and
  distinguish connector defects, harness controls, and DuckDB-specific expectations.
  The remaining 39 selected failures include named arguments that omit an
  earlier default, integer `AVG` result types, engine-specific SQL and settings,
  and equivalent predicate strings printed differently by the workers.
- **Function coverage.** Scalar, aggregate and table-in/table-out functions
  remain unsupported. They account for substantial parts of the upstream
  suite. Producer functions accept literal scalar arguments only.
- **Pushdown coverage.** Validate more types and expression combinations with
  the standard worker, including required filters and filtered scan planning.
  Join, aggregate, sort and limit pushdown remain unsupported.
- **Operational behavior.** Extend cancellation coverage to discovery and
  preparation; qualify remote attachment cleanup after forced disconnects,
  concurrent load, memory bounds and long-running scans. Each scan currently attaches
  independently; there is no connection pool.
- **Catalog behavior.** Qualify unknown versions, changes during a running query,
  resolved data/implementation versions and broader catalog metadata.
  Discovery is a snapshot. Constraint CHECK expressions use
  the pinned SDK's restricted numeric grammar and evaluation context.
- **Release criteria.** Publish a scoped compatibility matrix, reproducible
  conformance reports and performance measurements before describing this
  connector as production-ready. Writes and transactions would require a
  separately implemented and tested contract.

An unrun record is not a pass. A passing selected subset does not establish
compatibility with most of the full VGI suite.
