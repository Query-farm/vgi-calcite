package farm.query.vgicalcite;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.FunctionInfo;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;

import java.util.ArrayList;
import java.util.List;

/** Bind-time TABLE(...) macro, so a worker may choose output columns from literal arguments. */
final class VgiTableMacro implements TableMacro {
    private final VgiConfig config;
    private final FunctionInfo function;
    private final List<Field> arguments;

    private VgiTableMacro(VgiConfig config, FunctionInfo function, List<Field> arguments) {
        this.config = config;
        this.function = function;
        this.arguments = List.copyOf(arguments);
    }

    static List<VgiTableMacro> create(VgiConfig config, FunctionInfo function) {
        if (!"TABLE".equals(function.function_type()) || function.has_finalize()
                || function.input_from_args() || !function.required_secrets().isEmpty()
                || !function.required_settings().isEmpty()) {
            throw new UnsupportedOperationException("table input, required secrets/settings are not supported");
        }
        List<Field> fields = SchemaUtil.deserializeSchema(function.arguments()).getFields();
        for (Field field : fields) {
            var metadata = field.getMetadata();
            if ("true".equals(metadata.get("vgi_varargs")) || metadata.containsKey("vgi_type")) {
                throw new UnsupportedOperationException("variadic, any, or TABLE arguments are not supported");
            }
            // Bind arguments supported by ScalarValue's scalar writer; avoid pretending nested
            // SQL parameters work when their child schemas cannot be faithfully encoded.
            switch (field.getType().getTypeID()) {
                case Int, FloatingPoint, Bool, Utf8, Binary, Decimal -> ArrowTypes.type(new JavaTypeFactoryImpl(VgiTypeSystem.INSTANCE), field);
                default -> throw ArrowTypes.unsupported(field);
            }
        }
        List<VgiTableMacro> result = new ArrayList<>();
        int arity = fields.size();
        while (true) {
            result.add(new VgiTableMacro(config, function, fields.subList(0, arity)));
            if (arity == 0 || !fields.get(arity - 1).getMetadata().containsKey("vgi_default")) break;
            arity--;
        }
        // Separate arities preserve the distinction between omission and an explicit SQL NULL.
        return result;
    }

    @Override public List<FunctionParameter> getParameters() {
        List<FunctionParameter> result = new ArrayList<>();
        for (int i = 0; i < arguments.size(); i++) {
            int ordinal = i;
            Field field = arguments.get(i);
            result.add(new FunctionParameter() {
                @Override public int getOrdinal() { return ordinal; }
                @Override public String getName() { return field.getName(); }
                @Override public RelDataType getType(RelDataTypeFactory factory) {
                    // Calcite's macro literal converter accepts JavaType specifically. A
                    // plain SQL BIGINT type passes validation but converts every value to null.
                    var javaFactory = (org.apache.calcite.adapter.java.JavaTypeFactory) factory;
                    var sqlType = factory.createTypeWithNullability(ArrowTypes.type(factory, field), true);
                    return factory.createJavaType((Class<?>) javaFactory.getJavaClass(sqlType));
                }
                @Override public boolean isOptional() { return false; }
            });
        }
        return result;
    }

    @Override public TranslatableTable apply(List<? extends Object> values) {
        if (values.size() != arguments.size()) throw new IllegalArgumentException("Wrong VGI argument count");
        var encoder = ArgumentsEncoder.builder();
        for (int i = 0; i < arguments.size(); i++) {
            Field field = arguments.get(i);
            Object value = values.get(i);
            if (value instanceof org.apache.calcite.avatica.util.ByteString bytes) value = bytes.getBytes();
            var scalar = ScalarValue.of(field.getType(), value);
            if ("named".equals(field.getMetadata().get("vgi_arg"))) encoder.named(field.getName(), scalar);
            else encoder.positional(scalar);
        }
        VgiTable.Invocation invocation = new VgiTable.Invocation(function.name(), function.schema_path(),
                encoder.encode(), Boolean.TRUE.equals(function.projection_pushdown()));
        try (VgiSession session = VgiSession.open(config)) {
            var binding = VgiTable.bind(session, invocation);
            var table = VgiTable.function(config, invocation, SchemaUtil.deserializeSchema(binding.output_schema()));
            session.detach();
            return table;
        }
    }
}
