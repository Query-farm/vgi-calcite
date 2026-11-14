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

The Java suite now has 45 tests. The Python interoperability gate runs 89
upstream records plus 9 adapter-specific records over TCP and HTTP. The
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
`e75482339d07244105e4a040f998a7092b7402a5`; its two selected test files match the
development suite above.

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
  Current function scans do not push column selection. Join, aggregate, sort
  and limit pushdown remain unsupported.
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
