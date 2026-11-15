package farm.query.vgicalcite;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** JSON-lines bridge for executing upstream SQLLogicTest cases through real Calcite JDBC. */
public final class SqlLogicBridge {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("caseSensitive", "false");
        properties.setProperty("timeZone", "UTC");
        // Accept upstream syntax such as % and GROUP BY aliases using Calcite's
        // own conformance mode; execution still uses Calcite operators.
        properties.setProperty("conformance", "BABEL");
        try (Connection connection = DriverManager.getConnection("jdbc:vgi-calcite:", properties);
             var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
            String line;
            while ((line = input.readLine()) != null) {
                Map<String, Object> response = new LinkedHashMap<>();
                long started = System.nanoTime();
                try {
                    JsonNode request = JSON.readTree(line);
                    switch (request.path("op").asText("query")) {
                        case "attach" -> {
                            Map<String, Object> operands = JSON.convertValue(request.get("operand"),
                                    new com.fasterxml.jackson.core.type.TypeReference<>() {});
                            String alias = request.path("alias").asText();
                            if (root.getSubSchema(alias) != null) throw new IllegalArgumentException("Schema already attached: " + alias);
                            var schema = new VgiSchemaFactory().create(root, alias, operands);
                            root.add(alias, schema);
                            response.put("unsupported_functions", ((VgiSchema) schema).unsupportedFunctions());
                            Map<String, List<String>> parameters = new LinkedHashMap<>();
                            functionParameters(schema, alias, parameters);
                            response.put("function_parameters", parameters);
                        }
                        case "detach" -> {
                            if (!org.apache.calcite.jdbc.CalciteSchema.from(root).removeSubSchema(request.path("alias").asText())) {
                                throw new IllegalArgumentException("Unknown schema: " + request.path("alias").asText());
                            }
                        }
                        case "query" -> {
                            try (var statement = connection.createStatement()) {
                                statement.setQueryTimeout(request.path("timeout").asInt(30));
                                boolean hasRows = statement.execute(request.path("sql").asText());
                                if (hasRows) {
                                    try (var result = statement.getResultSet()) {
                                        List<String> columns = new ArrayList<>();
                                        List<Integer> types = new ArrayList<>();
                                        int width = result.getMetaData().getColumnCount();
                                        for (int i = 1; i <= width; i++) {
                                            columns.add(result.getMetaData().getColumnLabel(i));
                                            types.add(result.getMetaData().getColumnType(i));
                                        }
                                        List<List<String>> rows = new ArrayList<>();
                                        int limit = request.path("max_rows").asInt(100_000);
                                        while (result.next()) {
                                            if (rows.size() == limit) throw new IllegalStateException("Conformance result row limit exceeded");
                                            List<String> row = new ArrayList<>();
                                            for (int i = 1; i <= width; i++) {
                                                String value = result.getString(i);
                                                row.add(value == null ? "NULL" : value);
                                            }
                                            rows.add(row);
                                        }
                                        response.put("columns", columns);
                                        response.put("jdbc_types", types);
                                        response.put("rows", rows);
                                    }
                                } else response.put("update_count", statement.getUpdateCount());
                            }
                        }
                        default -> throw new IllegalArgumentException("Unknown operation");
                    }
                    response.put("ok", true);
                } catch (Exception failure) {
                    response.put("ok", false);
                    List<String> causes = new ArrayList<>();
                    for (Throwable cause = failure; cause != null && causes.size() < 12; cause = cause.getCause()) {
                        causes.add(cause.getClass().getSimpleName() + ": " + cause.getMessage());
                    }
                    response.put("error", String.join("\n", causes));
                }
                response.put("elapsed_ms", (System.nanoTime() - started) / 1_000_000);
                System.out.println(JSON.writeValueAsString(response));
                System.out.flush();
            }
        }
    }

    private static void functionParameters(org.apache.calcite.schema.Schema schema, String path,
                                           Map<String, List<String>> result) {
        for (String name : schema.getFunctionNames()) {
            for (var function : schema.getFunctions(name)) {
                List<String> names = function.getParameters().stream().map(p -> p.getName()).toList();
                String key = (path + "." + name).toLowerCase(java.util.Locale.ROOT);
                if (!result.containsKey(key) || result.get(key).size() < names.size()) result.put(key, names);
            }
        }
        for (String child : schema.getSubSchemaNames()) {
            functionParameters(schema.getSubSchema(child), path + "." + child, result);
        }
    }
}
