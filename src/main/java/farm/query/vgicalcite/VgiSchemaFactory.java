package farm.query.vgicalcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import java.util.Map;

/** Entry point for a Calcite JSON model's custom schema. */
public final class VgiSchemaFactory implements SchemaFactory {
    public static final VgiSchemaFactory INSTANCE = new VgiSchemaFactory();

    @Override public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        java.util.List<String> path = java.util.List.of();
        if (operand.containsKey("schemaPath")) {
            if (!(operand.get("schemaPath") instanceof java.util.List<?> list)
                    || list.stream().anyMatch(v -> !(v instanceof String))) {
                throw new IllegalArgumentException("schemaPath must be an array of schema names");
            }
            path = list.stream().map(String.class::cast).toList();
        }
        return VgiSchema.connect(VgiConfig.from(operand)).mount(path);
    }
}
