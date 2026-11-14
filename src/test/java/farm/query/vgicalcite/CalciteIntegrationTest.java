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

    @Test void unsupportedBranchPredicatesAndRequiredFiltersFailExplicitly() {
        Exception branch = assertThrows(Exception.class, () -> query("select * from vgi.data.filtered_branch"));
        assertTrue(branch.toString().contains("branch_filter"), branch.toString());
        Exception required = assertThrows(Exception.class, () -> query("select * from vgi.data.requires_filter where id > 2"));
        assertTrue(required.toString().contains("filter planning"), required.toString());
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
    }
}
