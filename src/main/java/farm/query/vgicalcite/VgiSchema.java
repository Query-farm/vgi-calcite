package farm.query.vgicalcite;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A metadata snapshot. It holds no open connections; scans attach independently. */
public final class VgiSchema extends AbstractSchema {
    private final Map<String, Schema> schemas = new LinkedHashMap<>();
    private Map<String, Table> tables = Map.of();
    private Multimap<String, Function> functions = ImmutableMultimap.of();
    private final List<String> unsupportedFunctions = new ArrayList<>();

    private VgiSchema() {}

    public static VgiSchema connect(VgiConfig config) {
        VgiSchema root = new VgiSchema();
        try (VgiSession session = VgiSession.open(config)) {
            long catalogVersion = session.attachment.catalog_version();
            List<SchemaInfo> paths = session.service.catalog_schemas(session.handle(), null, null).items().stream()
                    .map(item -> RecordCodec.deserializeFromBytes(item, SchemaInfo.class)).toList();
            // Functions can back a table in a different schema. Keep all dispatch coordinates.
            List<FunctionInfo> allFunctions = new ArrayList<>();
            for (SchemaInfo info : paths) {
                List<FunctionInfo> functions = session.service.catalog_schema_contents_functions(
                        session.handle(), info.path(), "TABLE_FUNCTION", null, null).items().stream()
                        .map(item -> RecordCodec.deserializeFromBytes(item, FunctionInfo.class)).toList();
                allFunctions.addAll(functions);
            }
            Map<List<String>, farm.query.vgi.protocol.TableInfo> allTables = new LinkedHashMap<>();
            for (SchemaInfo info : paths) {
                for (var table : TableInfoDecoder.decodeAll(session.service.catalog_schema_contents_tables(
                        session.handle(), info.path(), null, null).items())) {
                    if (!table.schema_path().equals(info.path())) {
                        throw new IllegalStateException("VGI table schema path differs from its enumeration path: " + table.name());
                    }
                    if (allTables.put(tableKey(info.path(), table.name()), table) != null) {
                        throw new IllegalStateException("Duplicate VGI table: " + info.path() + "." + table.name());
                    }
                }
            }
            for (SchemaInfo info : paths) {
                VgiSchema schema = root.path(info.path());
                Map<String, Table> tables = new LinkedHashMap<>();
                for (var table : allTables.values()) {
                    if (!table.schema_path().equals(info.path())) continue;
                    VgiCatalogMetadata.validate(table, session.selectedCatalogCapabilities,
                            (path, name) -> allTables.get(tableKey(path, name)));
                    tables.put(table.name(), VgiTable.catalog(config, catalogVersion, table, allFunctions));
                }
                schema.tables = Collections.unmodifiableMap(tables);
            }
            // Use the object's full authoritative path, including when a worker's schema
            // listing reports only leaf names for its legacy registration API.
            Map<VgiSchema, ImmutableMultimap.Builder<String, Function>> builders = new LinkedHashMap<>();
            for (FunctionInfo fn : allFunctions) {
                if (FunctionInfo.VISIBILITY_TABLE_ONLY.equals(fn.visibility())) continue;
                VgiSchema schema = root.path(fn.schema_path());
                var functions = builders.computeIfAbsent(schema, ignored -> ImmutableMultimap.builder());
                try {
                    for (VgiTableMacro macro : VgiTableMacro.create(config, catalogVersion, fn)) functions.put(fn.name(), macro);
                } catch (UnsupportedOperationException e) {
                    String reason = String.join(".", fn.schema_path()) + "." + fn.name() + ": " + e.getMessage();
                    schema.unsupportedFunctions.add(reason);
                    if (schema != root) root.unsupportedFunctions.add(reason);
                }
            }
            builders.forEach((schema, builder) -> schema.functions = builder.build());
            session.verifyCatalogVersion();
            session.detach();
        }
        return root;
    }

    private static List<String> tableKey(List<String> path, String name) {
        var key = new ArrayList<>(path);
        key.add(name);
        return List.copyOf(key);
    }

    private VgiSchema path(List<String> path) {
        VgiSchema schema = this;
        for (String component : path) {
            schema = (VgiSchema) schema.schemas.computeIfAbsent(component, ignored -> new VgiSchema());
        }
        return schema;
    }

    VgiSchema mount(List<String> path) {
        VgiSchema schema = this;
        for (String component : path) {
            schema = (VgiSchema) schema.schemas.get(component);
            if (schema == null) throw new IllegalArgumentException("VGI schemaPath does not exist: " + path);
        }
        return schema;
    }

    /** Functions omitted because their parameter shapes are outside the adapter's scope. */
    public List<String> unsupportedFunctions() { return List.copyOf(unsupportedFunctions); }
    @Override protected Map<String, Schema> getSubSchemaMap() { return Collections.unmodifiableMap(schemas); }
    @Override protected Map<String, Table> getTableMap() { return tables; }
    @Override protected Multimap<String, Function> getFunctionMultimap() { return functions; }
    @Override public boolean isMutable() { return false; }
}
