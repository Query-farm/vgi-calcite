package farm.query.vgicalcite;

import farm.query.vgi.client.ScanBranchesDecoder;
import farm.query.vgi.client.ScanFunctionArguments;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.ArrayList;
import java.util.List;

/** Read-only VGI table with capability-gated projection and predicate pushdown. */
public final class VgiTable extends AbstractTable implements ProjectableFilterableTable, TranslatableTable {
    record Invocation(String function, List<String> path, byte[] arguments, boolean projection, boolean filtering) {}
    final VgiConfig config;
    final Schema columns;
    private final TableInfo table;
    private final Invocation invocation;
    private final List<FunctionInfo> functions;

    private VgiTable(VgiConfig config, Schema columns, TableInfo table, Invocation invocation, List<FunctionInfo> functions) {
        this.config = config;
        this.columns = columns;
        this.table = table;
        this.invocation = invocation;
        this.functions = List.copyOf(functions);
    }

    static VgiTable catalog(VgiConfig config, TableInfo table, List<FunctionInfo> functions) {
        return new VgiTable(config, SchemaUtil.deserializeSchema(table.columns()), table, null, functions);
    }

    static VgiTable function(VgiConfig config, Invocation invocation, Schema columns) {
        return new VgiTable(config, columns, null, invocation, List.of());
    }

    @Override public RelDataType getRowType(RelDataTypeFactory factory) { return ArrowTypes.rowType(factory, columns); }

    @Override public Statistic getStatistic() {
        return table != null && table.cardinality_estimate() != null
                ? Statistics.of(table.cardinality_estimate().doubleValue(), List.of()) : Statistics.UNKNOWN;
    }

    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        if (invocation != null) {
            VgiFunctionScan.registerRules(context.getCluster().getPlanner());
            return new VgiFunctionScan(context.getCluster(), relOptTable, this);
        }
        return LogicalTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
    }

    /** Runtime entry point used by generated Calcite code for a bound table macro. */
    public Enumerable<Object[]> scanAll(DataContext context) { return scan(context, new ArrayList<>(), null); }

    /** Runtime entry point for function scans; their enclosing Calcite operator rechecks the filters. */
    public Enumerable<Object[]> scanFiltered(DataContext context, List<RexNode> filters) {
        return scan(context, filters, null);
    }

    String invocationKey() {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(invocation.arguments()));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    @Override public Enumerable<Object[]> scan(DataContext context, List<RexNode> filters, int[] projects) {
        // Retain all originals for Calcite to recheck, including advisory scan-planning filters.
        List<RexNode> predicates = List.copyOf(filters);
        int[] projection = projects == null ? java.util.stream.IntStream.range(0, columns.getFields().size()).toArray()
                : projects.clone();
        return new AbstractEnumerable<>() {
            @Override public Enumerator<Object[]> enumerator() {
                return new VgiEnumerator(VgiTable.this, context, projection, predicates);
            }
        };
    }

    List<Invocation> resolve(VgiSession session, VgiFilters filters) {
        if (invocation != null) return List.of(invocation);
        requireFilters(filters);
        boolean filteredPlanning = TableInfo.SCAN_PLANNING_FILTERED.equals(table.scan_planning());
        if (table.scan_function() != null && table.scan_function().length != 0) {
            var scan = RecordCodec.deserializeFromBytes(table.scan_function(), TableScanFunctionGetResponse.class);
            requireNoExtensions(scan.required_extensions());
            return List.of(invocation(scan.function_name(), scan.schema_path(), scan.arguments()));
        }
        var branches = ScanBranchesDecoder.decode(session.service.catalog_table_scan_branches_get(
                session.handle(), table.schema_path(), table.name(), null, null, null,
                filteredPlanning ? filters.encode() : null, null));
        requireNoExtensions(branches.requiredExtensions());
        if (branches.branches().isEmpty() && !filteredPlanning) {
            throw new IllegalStateException("Bind-time VGI table has no scan branches: " + table.name());
        }
        List<Invocation> result = new ArrayList<>();
        for (var branch : branches.branches()) {
            if (branch.branchFilter() != null && !branch.branchFilter().isBlank()) {
                throw new UnsupportedOperationException("VGI branch_filter SQL is not supported: " + table.name());
            }
            if (branch.sourceTable() != null || branch.formatName() != null) {
                throw new UnsupportedOperationException("VGI catalog/format scan branches are not supported: " + table.name());
            }
            result.add(invocation(branch.functionName(), branch.schemaPath(), branch.arguments()));
        }
        return result;
    }

    private void requireFilters(VgiFilters filters) {
        if (table == null) return;
        for (List<List<String>> alternatives : table.required_filters()) {
            if (alternatives.stream().noneMatch(filters::covers)) {
                throw new IllegalArgumentException("Table " + table.name()
                        + " requires a supported filter on one of " + alternatives);
            }
        }
    }

    void requireFilterDelivery(Invocation branch, VgiFilters filters) {
        // A filtered planner has already received the table predicates. Bind-time
        // tables must instead deliver each required group to the actual scan.
        if (table == null || table.required_filters().isEmpty()
                || TableInfo.SCAN_PLANNING_FILTERED.equals(table.scan_planning())) return;
        List<String> names = columns.getFields().stream().map(org.apache.arrow.vector.types.pojo.Field::getName).toList();
        boolean delivered = branch.filtering() && table.required_filters().stream().allMatch(group ->
                group.stream().anyMatch(path -> path.size() == 1 && filters.coversIndex(names.indexOf(path.getFirst()))));
        if (!delivered) {
            throw new UnsupportedOperationException("Table " + table.name()
                    + " requires filters that cannot be delivered through compatible filter pushdown");
        }
    }

    private Invocation invocation(String name, List<String> path, byte[] arguments) {
        List<FunctionInfo> matches = functions.stream().filter(f -> f.name().equals(name)
                && (path == null || path.equals(f.schema_path()))).toList();
        // A path-less dispatch is legal, but guessing capability flags across overloads is not.
        boolean projection = matches.size() == 1 && Boolean.TRUE.equals(matches.getFirst().projection_pushdown());
        boolean filtering = matches.size() == 1 && VgiFilters.supported(matches.getFirst());
        return new Invocation(name, path, ScanFunctionArguments.toBindArguments(arguments), projection, filtering);
    }

    private static void requireNoExtensions(List<String> extensions) {
        if (extensions != null && !extensions.isEmpty()) {
            throw new UnsupportedOperationException("VGI scan requires engine extensions: " + extensions);
        }
    }

    static BindRequest request(VgiSession session, Invocation invocation) {
        return new BindRequest(invocation.function(), invocation.arguments(), "TABLE", null, null, null,
                session.handle(), null, false, null, null, null, null, invocation.path());
    }

    static BindResponse bind(VgiSession session, Invocation invocation) {
        BindResponse bound = session.service.bind(request(session, invocation), null);
        if (!bound.lookup_secret_types().isEmpty() || !bound.lookup_scopes().isEmpty() || !bound.lookup_names().isEmpty()) {
            throw new UnsupportedOperationException("VGI bind requested secrets; secret resolution is not implemented");
        }
        return bound;
    }
}
