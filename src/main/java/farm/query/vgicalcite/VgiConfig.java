package farm.query.vgicalcite;

import java.util.List;
import java.util.Map;

/** Immutable connection configuration shared by discovery and independent scans. */
public record VgiConfig(String location, List<String> command, String catalog,
                        String bearerTokenEnv, Map<String, Object> attachOptions,
                        String dataVersion, String implementationVersion) {
    public VgiConfig {
        command = command == null ? List.of() : List.copyOf(command);
        attachOptions = attachOptions == null ? Map.of() : Map.copyOf(attachOptions);
        if ((location == null || location.isBlank()) == command.isEmpty()) {
            throw new IllegalArgumentException("Specify exactly one of location or command (an argv array)");
        }
        if (catalog == null || catalog.isBlank()) {
            throw new IllegalArgumentException("catalog must name the worker's VGI catalog");
        }
        if (command.stream().anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("command must contain nonempty strings");
        }
        if (location != null && !(location.startsWith("http://") || location.startsWith("https://")
                || location.startsWith("tcp://") || location.startsWith("unix://"))) {
            throw new IllegalArgumentException("location must use http(s)://, tcp:// or unix://; use command for a subprocess");
        }
    }

    /** Parse a Calcite JSON model's operand object. */
    public static VgiConfig from(Map<String, Object> operands) {
        var allowed = java.util.Set.of("location", "command", "catalog", "bearerTokenEnv",
                "attachOptions", "dataVersion", "implementationVersion", "schemaPath");
        for (String key : operands.keySet()) {
            // Calcite injects modelUri/baseDirectory/tables automatically.
            if (!allowed.contains(key) && !key.equals("modelUri") && !key.equals("baseDirectory") && !key.equals("tables")) {
                throw new IllegalArgumentException("Unknown VGI operand: " + key);
            }
        }
        List<String> command = List.of();
        if (operands.containsKey("command")) {
            if (!(operands.get("command") instanceof List<?> list)
                    || list.stream().anyMatch(v -> !(v instanceof String))) {
                throw new IllegalArgumentException("command must be an array of strings");
            }
            command = list.stream().map(String.class::cast).toList();
        }
        Map<String, Object> options = Map.of();
        if (operands.containsKey("attachOptions")) {
            if (!(operands.get("attachOptions") instanceof Map<?, ?> map)
                    || map.keySet().stream().anyMatch(k -> !(k instanceof String))) {
                throw new IllegalArgumentException("attachOptions must be an object");
            }
            var copy = new java.util.LinkedHashMap<String, Object>();
            map.forEach((k, v) -> copy.put((String) k, v));
            options = copy;
        }
        return new VgiConfig(string(operands, "location"), command, string(operands, "catalog"),
                string(operands, "bearerTokenEnv"), options, string(operands, "dataVersion"),
                string(operands, "implementationVersion"));
    }

    private static String string(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value != null && !(value instanceof String)) {
            throw new IllegalArgumentException(key + " must be a string");
        }
        return (String) value;
    }
}
