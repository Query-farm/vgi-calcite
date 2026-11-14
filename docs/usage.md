# Usage guide

Start with the [README quickstart](../README.md#quickstart) to build the adapter
and query the included demo worker.

## Connection options

Configure these values in the `operand` object of a Calcite JSON model.
Supply exactly one of `location` and `command`.

| Operand | Meaning |
| --- | --- |
| `catalog` | Required catalog name advertised by the worker. |
| `location` | `http://`, `https://`, `tcp://host:port`, or `unix:///absolute/path`. |
| `command` | Subprocess argument array, such as `["/absolute/path/to/worker"]`. Add arguments required by your worker. |
| `schemaPath` | Optional array such as `["data"]` to mount a single worker schema. Omit it to mount the full catalog with nested schemas. |
| `bearerTokenEnv` | Name of an environment variable containing an HTTP bearer token, read when each HTTP connection opens. |
| `attachOptions` | Object of scalar worker attach options, encoded by the SDK. |
| `dataVersion` | Optional requested VGI data-version specification. |
| `implementationVersion` | Optional requested VGI implementation version. |

Subprocess arguments are passed directly to `ProcessBuilder`. There is no
shell expansion of `~`, environment variables, or wildcards. The included Java
test worker uses stdio by default; omit its HTTP/TCP/Unix transport flags to run
it as a subprocess.

For HTTP authentication, set the token in your environment and reference the
variable by name:

```json
{
  "location": "https://worker.example.com",
  "catalog": "my_catalog",
  "bearerTokenEnv": "VGI_TOKEN"
}
```

This configures transport authentication. Resolving secrets requested by a
worker during function binding is not supported.

## JDBC tools and schema mounting

The model's `name` is a local SQL alias. With `"name": "vgi"`, mounting a full
catalog exposes a worker table named `data.numbers` as `vgi.data.numbers`.

Calcite 1.42's standard JDBC metadata enumerates tables in root schemas; it does
not recursively list tables in nested schemas. For tools that browse tables
through `DatabaseMetaData`, mount the worker schema directly:

```json
{
  "version": "1.0",
  "schemas": [
    {
      "name": "vgi",
      "type": "custom",
      "factory": "farm.query.vgicalcite.VgiSchemaFactory",
      "operand": {
        "location": "http://127.0.0.1:8000",
        "catalog": "demo",
        "schemaPath": ["data"]
      }
    }
  ]
}
```

This exposes `vgi.numbers` to both SQL queries and JDBC metadata. Use multiple
path components, such as `["sales", "reports"]`, for a deeper worker schema.
Both full-catalog and single-schema mounts support normal SQL queries.

## Embed in a Java application

The adapter is not published to Maven Central. Install it in your local Maven
repository:

```sh
./mvnw install
```

Add the dependency to your application:

```xml
<dependency>
  <groupId>farm.query</groupId>
  <artifactId>vgi-calcite</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Load a model through Calcite's JDBC driver:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

// Inside your application's entry point:
Properties properties = new Properties();
properties.setProperty("model", "/absolute/path/to/model.json");
properties.setProperty("caseSensitive", "false");
properties.setProperty("timeZone", "UTC");
properties.setProperty("typeSystem", "farm.query.vgicalcite.VgiTypeSystem");

try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
     Statement statement = connection.createStatement();
     ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM vgi.data.numbers")) {
  rows.next();
  System.out.println(rows.getLong(1));
}
```

Use these JVM flags in the embedding application:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
--enable-native-access=ALL-UNNAMED
-Dio.netty.noUnsafe=false
```

The packaged query runner sets case-insensitive identifier lookup, UTC, and
`VgiTypeSystem` automatically.

For programmatic setup, create a schema with
`VgiSchema.connect(VgiConfig.from(operandMap))` and add it to a
`CalciteConnection`'s root schema. The schema's `unsupportedFunctions()` method
reports table-function signatures omitted from discovery.

## Table-function arguments

Table functions accept literal scalar arguments, including SQL `NULL`.
Omitted trailing defaults and explicit `NULL` remain distinct. For named
arguments, use the worker's exact names in double quotes:

```sql
SELECT COUNT(*)
FROM TABLE(vgi.main.sequence("count" => 7, "batch_size" => 2));
```

Prepared statements containing literal table-function arguments can be executed
repeatedly. Dynamic prepared parameters (`?`) in those calls are not supported.

## Types and execution limits

### Arrow types

Supported types include booleans, signed and unsigned integers, floating-point
numbers, strings, binary values, decimals, dates, times, timestamps, arrays,
structs, maps, and top-level dictionaries. Unknown types, nested dictionary
encodings, and Arrow logical extension types fail explicitly.

Unsigned integers are widened to a suitable Calcite type; `uint64` maps to
`DECIMAL(20,0)`. `VgiTypeSystem` raises Calcite's decimal precision limit from
19 to 38 digits. Use it for `uint64` and decimal128 columns above 19 digits.
Decimal256 is not supported.

Time and timestamp values use **millisecond precision**. Microsecond and
nanosecond values are floored to milliseconds, including values before the Unix
epoch. Zoned timestamps use Calcite's `TIMESTAMP WITH LOCAL TIME ZONE`.

### Connections and catalog metadata

Catalog metadata is a snapshot taken when the model loads. Reconnect to refresh
it. Each scan attaches and binds on its own connection, so scans do not share a
transaction or snapshot. Set `dataVersion` when the worker supports stable
versioned reads.

A subprocess command starts a worker for each scan. Use a persistent HTTP,
TCP, or Unix-socket worker to avoid process startup overhead. Connection pooling
and the `launch:` transport are not implemented.

Catalog tables can use an inline scan function or one or more function scan
branches. Multiple branches are read sequentially with `UNION ALL` semantics.
Distributed split planning and native-file scan delegation are not supported.

### Worker features

The adapter supports column selection on catalog-table scans when the backing
function advertises it. Calcite handles other projections, filters, joins,
aggregates, sorts, and limits.

The following worker features are not supported:

- VGI scalar and aggregate functions, table-in/out transforms, and views or
  macros written in the worker's SQL dialect.
- Table, `any`, or variadic function arguments, required session settings,
  and secrets requested during binding.
- Required-filter tables, filtered scan planning, and SQL `branch_filter`
  clauses.
- Format or native scan delegation, such as `read_parquet`, companion catalogs,
  and scans requiring engine extensions.

Unsupported scan requirements are rejected rather than approximated with a
partial scan. The adapter advertises no advanced execution capabilities.

### Timeouts and cancellation

TCP connections have a 10-second connect timeout and a 60-second socket read
timeout. HTTP uses the SDK's timeouts. Subprocess and Unix-socket reads have no
adapter deadline.

Cancellation is checked between rows and batches. Closing a scan early sends
the SDK's stream cancellation signal and closes the transport; cancellation
does not interrupt a blocked subprocess read.
