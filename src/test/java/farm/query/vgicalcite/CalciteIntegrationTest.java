package farm.query.vgicalcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.jupiter.api.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
class CalciteIntegrationTest {
    private static Process worker;
    private static String location;
    private Connection connection;

    static List<String> command() {
        return List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED",
                "-Dio.netty.noUnsafe=false", "-Dorg.slf4j.simpleLogger.defaultLogLevel=error", "-cp",
                System.getProperty("java.class.path"), FixtureWorker.class.getName());
    }

    @BeforeAll static void startWorker() throws Exception {
        List<String> command = new ArrayList<>(command());
        command.addAll(List.of("--tcp", "127.0.0.1:0"));
        worker = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            String line;
            try {
                line = executor.submit(() -> new BufferedReader(new InputStreamReader(worker.getInputStream())).readLine())
                        .get(20, TimeUnit.SECONDS);
            } catch (Exception e) { worker.destroyForcibly(); throw e; }
            assertNotNull(line, "worker exited before opening its socket");
            assertTrue(line.startsWith("TCP:"), line);
            location = "tcp://" + line.substring(4);
        } catch (Exception e) {
            worker.destroyForcibly();
            throw e;
        }
    }

    @AfterAll static void stopWorker() throws Exception {
        if (worker != null) { worker.destroy(); if (!worker.waitFor(5, TimeUnit.SECONDS)) worker.destroyForcibly(); }
    }

    @BeforeEach void connect() throws Exception { connection = open(Map.of("location", location, "catalog", "demo")); }
    @AfterEach void disconnect() throws Exception { if (connection != null) connection.close(); }

    static Connection open(Map<String, Object> operand) throws Exception {
        String model = new ObjectMapper().writeValueAsString(Map.of("version", "1.0", "schemas", List.of(
                Map.of("name", "vgi", "type", "custom", "factory", VgiSchemaFactory.class.getName(), "operand", operand))));
        Properties properties = new Properties();
        properties.setProperty("model", "inline:" + model);
        properties.setProperty("caseSensitive", "false");
        properties.setProperty("timeZone", "UTC");
        properties.setProperty("typeSystem", VgiTypeSystem.class.getName());
        return DriverManager.getConnection("jdbc:calcite:", properties);
    }

    List<List<Object>> query(String sql) throws Exception {
        try (var statement = connection.createStatement(); var results = statement.executeQuery(sql)) {
            List<List<Object>> rows = new ArrayList<>();
            while (results.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= results.getMetaData().getColumnCount(); i++) row.add(results.getObject(i));
                rows.add(row);
            }
            return rows;
        }
    }

    @Test void discoversTablesThroughJdbcMetadata() throws Exception {
        connection.close();
        connection = open(Map.of("location", location, "catalog", "demo", "schemaPath", List.of("data")));
        try (var tables = connection.getMetaData().getTables(null, "vgi", "%", null)) {
            List<String> names = new ArrayList<>();
            while (tables.next()) names.add(tables.getString("TABLE_NAME"));
            assertTrue(names.contains("numbers"), names.toString());
        }
    }

    @Test void filtersSortsAndProjectsAcrossBatches() throws Exception {
        assertEquals(List.of(List.of("row-8", 8L), List.of("row-7", 7L), List.of("row-5", 5L), List.of("row-4", 4L)),
                query("select label, id from vgi.data.numbers where id >= 4 and label is not null order by id desc"));
    }

    @Test void aggregatesWithoutProjectedColumns() throws Exception {
        assertEquals(List.of(List.of(10L, 45L, 6L)), query("select count(*), sum(id), count(label) from vgi.data.numbers"));
        assertEquals(List.of(List.of(10L)), query("select count(*) from vgi.data.numbers"));
        assertEquals(List.of(List.of(0L)), query("select count(*) from vgi.data.\"empty\""));
    }

    @Test void projectionFallbackPreservesColumns() throws Exception {
        assertEquals(query("select label, id from vgi.data.numbers order by id"),
                query("select label, id from vgi.data.unprojected order by id"));
    }

    @Test void tableFunctionDefaultAndNamedArguments() throws Exception {
        assertEquals(List.of(List.of(17L, 136L)), query("select count(*), sum(id) from table(vgi.main.sequence(17))"));
        assertEquals(List.of(List.of(7L)), query("select count(*) from table(vgi.main.sequence(\"count\" => 7, \"batch_size\" => 2))"));
    }

    @Test void explicitNullDoesNotOmitOrShiftArguments() throws Exception {
        assertEquals(List.of(Arrays.asList(null, 7L, 99L)), query("select * from table(vgi.main.echo(cast(null as bigint), 7))"));
        assertEquals(List.of(Arrays.asList(2L, 7L, null)), query("select * from table(vgi.main.echo(2, 7, cast(null as bigint)))"));
    }

    @Test void nestedSchemaFunctions() throws Exception {
        connection.close();
        connection = open(Map.of("location", location, "catalog", "nested_demo"));
        assertEquals(List.of(List.of(4L)), query("select count(*) from table(vgi.nested.deep.nested_sequence(4))"));
    }

    @Test void readsAllScanBranches() throws Exception {
        assertEquals(List.of(List.of(5L, 4L)), query("select count(*), sum(id) from vgi.data.combined"));
    }

    @Test void unsupportedBranchPredicatesAndMissingRequiredFiltersFailExplicitly() {
        Exception branch = assertThrows(Exception.class, () -> query("select * from vgi.data.filtered_branch"));
        assertTrue(branch.toString().contains("branch_filter"), branch.toString());
        Exception required = assertThrows(Exception.class, () -> query("select * from vgi.data.requires_filter"));
        assertTrue(required.toString().contains("requires a supported filter"), required.toString());
    }

    @Test void selfJoinUsesIndependentConnections() throws Exception {
        assertEquals(List.of(List.of(9L)), query("select count(*) from vgi.data.numbers a join vgi.data.numbers b on a.id + 1 = b.id"));
    }

    @Test void differentFunctionArgumentsStayDistinctDuringOptimization() throws Exception {
        assertEquals(List.of(List.of(8L, 13L)), query("select count(*), sum(id) from ("
                + "select id from table(vgi.main.sequence(3)) union all select id from table(vgi.main.sequence(5)))"));
        assertEquals(List.of(List.of(15L)), query("select count(*) from table(vgi.main.sequence(3)) a "
                + "cross join table(vgi.main.sequence(5)) b"));
    }

    @Test void preparedQueryCanExecuteAgain() throws Exception {
        try (var statement = connection.prepareStatement("select count(*) from table(vgi.main.sequence(8))")) {
            for (int i = 0; i < 3; i++) {
                try (var results = statement.executeQuery()) {
                    assertTrue(results.next());
                    assertEquals(8L, results.getLong(1));
                }
            }
        }
    }

    @Test void earlyLimitAndRepeatedExecutionLeaveWorkerUsable() throws Exception {
        assertEquals(List.of(List.of(0L)), query("select id from table(vgi.main.sequence(10000000)) fetch next 1 rows only"));
        for (int i = 0; i < 3; i++) assertEquals(List.of(List.of(10L)), query("select count(*) from vgi.data.numbers"));
    }

    @Test void workerFailureDoesNotPoisonNextQuery() throws Exception {
        Exception failure = assertThrows(Exception.class, () -> query("select * from table(vgi.main.sequence(-1))"));
        assertTrue(failure.toString().contains("nonnegative") || failure.getCause() != null);
        assertEquals(List.of(List.of(10L)), query("select count(*) from vgi.data.numbers"));
    }

    @Test void typedValuesSurviveJdbcAndSqlExpressions() throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(
                "select active, amount, payload, stamp from vgi.data.numbers where id = 2")) {
            assertTrue(result.next());
            assertTrue(result.getBoolean(1));
            assertEquals(new java.math.BigDecimal("2.50"), result.getBigDecimal(2));
            assertArrayEquals(new byte[] {2, 42}, result.getBytes(3));
            assertEquals(1999L, result.getTimestamp(4).getTime());
            assertFalse(result.next());
        }
        assertEquals(List.of(List.of(new java.math.BigDecimal("56.25"))), query("select sum(amount) from vgi.data.numbers"));
    }

    @Test void subprocessTransportUsesArgumentArray() throws Exception {
        connection.close();
        connection = open(Map.of("command", command(), "catalog", "demo"));
        assertEquals(List.of(List.of(10L)), query("select count(*) from vgi.data.numbers"));
        assertEquals(List.of(List.of(4L)), query("select id from vgi.data.filtered_sequence where id = 4"));
        assertEquals(List.of(List.of(8L)), query("select id from table(vgi.main.filtered_sequence(10)) where id = 8"));
    }

    private void resetStats() throws Exception { query("select * from table(vgi.main.scan_stats(true))"); }

    private void assertStats(long inits, long rows, long plans) throws Exception {
        assertEquals(List.of(List.of(inits, rows, plans)), query("select * from table(vgi.main.scan_stats(false))"));
    }

    private void assertRemote(String predicate, long transferred) throws Exception {
        var expected = query("select id from vgi.data.numbers where " + predicate + " order by id");
        resetStats();
        assertEquals(expected, query("select id from vgi.data.filtered_sequence where " + predicate + " order by id"), predicate);
        assertStats(1, transferred, 0);
    }

    @Test void comparisonsRangesAndMembershipFilterBeforeTransfer() throws Exception {
        assertRemote("id = 4", 1);
        assertRemote("4 < id and id <= 7", 3);
        assertRemote("id in (1, 4, 8)", 3);
        assertRemote("id in (1, cast(null as bigint))", 1);
        assertRemote("id between 2 and 5", 4);
        assertRemote("id <> 4", 9);
        assertRemote("id = 1 or id = 8", 2);
        assertRemote("id not in (1, 4, 8)", 7);
        assertRemote("id < -1", 0);
    }

    @Test void nullBooleanDecimalAndBinaryPredicatesFilterRemotely() throws Exception {
        assertRemote("label is null", 4);
        assertRemote("label is not null", 6);
        assertRemote("label = 'row-4'", 1);
        assertRemote("label <> 'row-4'", 5);
        assertRemote("label is null or label = 'row-4'", 5);
        assertRemote("active", 5);
        assertRemote("not active", 5);
        assertRemote("amount >= 5.00 and amount < 8.00", 3);
        assertRemote("payload = X'022A'", 1);
    }

    @Test void temporalPushdownPreservesMillisecondFlooringAndNegativeEpochs() throws Exception {
        assertRemote("stamp = timestamp '1970-01-01 00:00:01.999'", 1);
        assertRemote("stamp <> timestamp '1970-01-01 00:00:01.999'", 9);
        assertRemote("stamp > timestamp '1970-01-01 00:00:01.999'", 7);
        assertRemote("stamp >= timestamp '1970-01-01 00:00:01.999'", 8);
        assertRemote("stamp < timestamp '1970-01-01 00:00:01.999'", 2);
        assertRemote("stamp <= timestamp '1970-01-01 00:00:01.999'", 3);
        assertRemote("stamp = timestamp '1969-12-31 23:59:59.999'", 1);
        assertRemote("stamp < timestamp '1970-01-01 00:00:00'", 1);
        assertRemote("stamp_nano = timestamp '1969-12-31 23:59:59.999'", 1);
        assertRemote("stamp_nano >= timestamp '1970-01-01 00:00:01.999'", 8);
        assertRemote("event_day = date '1969-12-29'", 1);
        assertRemote("day_milli = date '1969-12-29'", 1);
        assertRemote("clock = time '00:00:02'", 1);
    }

    @Test void unsignedBoundariesAndOutOfRangeConstantsNeverWrap() throws Exception {
        assertRemote("u64 = 18446744073709551615", 1);
        assertRemote("u64 > 18446744073709551612", 3);
        assertRemote("small < 0", 5);
        assertRemote("cast(small as integer) < 0", 5);
        resetStats();
        assertEquals(List.of(List.of(0L)), query("select count(*) from vgi.data.filtered_sequence where small = 256"));
        assertEquals(10L, query("select * from table(vgi.main.scan_stats(false))").getFirst().get(1));
        resetStats();
        assertEquals(query("select id from vgi.data.numbers where amount < 2.501 order by id"),
                query("select id from vgi.data.filtered_sequence where amount < 2.501 order by id"));
    }

    @Test void unsupportedExpressionsRemainLocalWithoutDroppingOtherDisjuncts() throws Exception {
        assertRemote("id >= 4 and mod(id, 2) = 0", 6);
        resetStats();
        assertEquals(List.of(List.of(1L), List.of(8L)), query(
                "select id from vgi.data.filtered_sequence where id = 1 or label = 'row-8' order by id"));
        assertEquals(10L, query("select * from table(vgi.main.scan_stats(false))").getFirst().get(1));
        resetStats();
        assertEquals(query("select id from vgi.data.numbers where cast(amount as integer) = 3 order by id"),
                query("select id from vgi.data.filtered_sequence where cast(amount as integer) = 3 order by id"));
        assertStats(0, 10, 0);
        resetStats();
        assertEquals(query("select id from vgi.data.numbers where floating > 4 order by id"),
                query("select id from vgi.data.filtered_sequence where floating > 4 order by id"));
        assertEquals(10L, query("select * from table(vgi.main.scan_stats(false))").getFirst().get(1));
        resetStats();
        assertEquals(query("select id from vgi.data.numbers where label > 'row-4' order by id"),
                query("select id from vgi.data.filtered_sequence where label > 'row-4' order by id"));
        assertEquals(10L, query("select * from table(vgi.main.scan_stats(false))").getFirst().get(1));
    }

    @Test void capabilityMismatchFallsBackAndAdvisoryWorkersAreRechecked() throws Exception {
        resetStats();
        assertEquals(List.of(List.of(4L)), query("select id from vgi.data.foreign_profile where id = 4"));
        assertStats(0, 10, 0);
        resetStats();
        assertEquals(List.of(List.of(4L)), query("select id from vgi.data.advisory_sequence where id = 4"));
        assertStats(1, 10, 0);
        assertEquals(List.of(List.of(4L)), query("select id from vgi.data.numbers where id = 4"));
    }

    @Test void filtersUseBindIndexesAndNamesAfterProjectionAndCatalogRenaming() throws Exception {
        resetStats();
        assertEquals(List.of(List.of("row-4", 4L, "row-4")), query(
                "select renamed_label, renamed_id, renamed_label from vgi.data.renamed where renamed_id = 4"));
        assertStats(1, 1, 0);
        resetStats();
        assertEquals(List.of(List.of("row-4", 4L)), query("select label, id from vgi.data.filtered_unprojected where id = 4"));
        assertStats(1, 1, 0);
        resetStats();
        assertEquals(List.of(List.of(1L)), query("select count(*) from vgi.data.filtered_sequence where label = 'row-4'"));
        assertStats(1, 1, 0);
    }

    @Test void functionScansPushFiltersAndKeepDistinctPredicates() throws Exception {
        resetStats();
        assertEquals(List.of(List.of(4L)), query("select id from table(vgi.main.filtered_sequence(10)) where id = 4"));
        assertStats(1, 1, 0);
        resetStats();
        assertEquals(List.of(List.of(2L, 12L)), query("select count(*), sum(id) from ("
                + "select id from table(vgi.main.filtered_sequence(10)) where id = 4 union all "
                + "select id from table(vgi.main.filtered_sequence(10)) where id = 8)"));
        assertStats(2, 2, 0);
    }

    @Test void preparedWhereParametersAreBoundForEveryExecution() throws Exception {
        for (String source : List.of("vgi.data.filtered_sequence", "table(vgi.main.filtered_sequence(10))")) {
            try (var statement = connection.prepareStatement("select id from " + source + " where id >= ? and id < ? order by id")) {
                for (int lower : List.of(2, 7)) {
                    resetStats();
                    statement.setLong(1, lower);
                    statement.setLong(2, lower + 2);
                    try (var rows = statement.executeQuery()) {
                        assertTrue(rows.next()); assertEquals(lower, rows.getLong(1));
                        assertTrue(rows.next()); assertEquals(lower + 1, rows.getLong(1));
                        assertFalse(rows.next());
                    }
                    assertStats(1, 2, 0);
                }
            }
        }
    }

    @Test void preparedScalarAndNullParametersPreserveSqlResults() throws Exception {
        for (String column : List.of("label", "payload", "amount")) {
            resetStats();
            try (var statement = connection.prepareStatement("select id from vgi.data.filtered_sequence where " + column + " = ?")) {
                switch (column) {
                    case "label" -> statement.setString(1, "row-2");
                    case "payload" -> statement.setBytes(1, new byte[] {2, 42});
                    case "amount" -> statement.setBigDecimal(1, new java.math.BigDecimal("2.50"));
                }
                try (var rows = statement.executeQuery()) {
                    assertTrue(rows.next()); assertEquals(2L, rows.getLong(1)); assertFalse(rows.next());
                }
                assertStats(1, 1, 0);
                resetStats();
                statement.setNull(1, java.sql.Types.NULL);
                try (var rows = statement.executeQuery()) { assertFalse(rows.next()); }
                assertStats(0, 10, 0);
            }
        }
    }

    @Test void requiredFiltersAreDeliveredOrRejectedBeforeScanning() throws Exception {
        resetStats();
        assertEquals(List.of(List.of(3L), List.of(4L)), query("select id from vgi.data.requires_filter where id > 2 order by id"));
        assertStats(1, 2, 0);
        assertThrows(Exception.class, () -> query("select * from vgi.data.requires_filter where abs(id) = 2"));
        Exception unsupported = assertThrows(Exception.class, () -> query(
                "select * from vgi.data.requires_filter_unsupported where id = 2"));
        assertTrue(unsupported.toString().contains("compatible filter pushdown"), unsupported.toString());
    }

    @Test void filteredPlanningReceivesPredicatesAndCanReturnNoBranches() throws Exception {
        resetStats();
        assertEquals(List.of(List.of(8L), List.of(9L)), query("select id from vgi.data.planned where id >= 8 order by id"));
        assertStats(1, 2, 1);
        resetStats();
        assertEquals(List.of(List.of(0L)), query("select count(*) from vgi.data.planned where id >= 100"));
        assertStats(0, 0, 1);
    }

    @Test void everyScanBranchReceivesFilters() throws Exception {
        resetStats();
        assertEquals(List.of(List.of(4L, 11L)), query("select count(*), sum(id) from vgi.data.filtered_combined where id >= 2"));
        assertStats(2, 4, 0);
    }
}
