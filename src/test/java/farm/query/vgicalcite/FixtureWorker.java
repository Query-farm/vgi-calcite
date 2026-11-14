package farm.query.vgicalcite;

import farm.query.vgi.Worker;
import farm.query.vgi.CatalogInterface;
import farm.query.vgi.protocol.*;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ScanBranch;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Real VGI worker launched in another JVM; no mocked client or SQL engine. */
public final class FixtureWorker {
    static final Schema COLUMNS = Schemas.of(
            Schemas.nullable("id", Schemas.INT64),
            Schemas.nullable("label", Schemas.UTF8),
            Schemas.nullable("active", new ArrowType.Bool()),
            Schemas.nullable("amount", new ArrowType.Decimal(12, 2, 128)),
            Schemas.nullable("payload", new ArrowType.Binary()),
            Schemas.nullable("stamp", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));

    public static void main(String[] args) {
        byte[] schema = SchemaUtil.serializeSchema(COLUMNS);
        Worker worker = Worker.builder().catalogName("demo").defaultSchema("main")
                .registerTable("main", new Sequence("sequence", true))
                .registerTable("main", new Sequence("unprojected", false))
                .registerTable("main", new Echo())
                .registerCatalogTable(CatalogTable.builder("data", "numbers", schema)
                        .scanFunction("sequence", List.of(10L), Map.of("batch_size", 3L))
                        .exposeScan(true).cardinality(10, 10).build())
                .registerCatalogTable(CatalogTable.builder("data", "unprojected", schema)
                        .scanFunction("unprojected", List.of(10L), Map.of("batch_size", 3L))
                        .exposeScan(true).build())
                .registerCatalogTable(CatalogTable.builder("data", "empty", schema)
                        .scanFunction("sequence", List.of(0L), Map.of()).exposeScan(true).build());
        worker.registerMultiBranchTable(CatalogTable.builder("data", "combined", schema).build(),
                List.of(ScanBranch.of("sequence", 3L), ScanBranch.of("sequence", 2L)), List.of());
        worker.registerMultiBranchTable(CatalogTable.builder("data", "filtered_branch", schema).build(),
                List.of(ScanBranch.filtered("sequence", "id >= 2", 5L)), List.of());
        worker.registerCatalogTable(CatalogTable.builder("data", "requires_filter", schema)
                .scanFunction("sequence", List.of(5L), Map.of()).exposeScan(true)
                .requiredFilters(List.of(List.of(List.of("id")))).build());
        Sequence nested = new Sequence("nested_sequence", true);
        worker.registerCatalog(new CatalogInterface() {
            @Override public String name() { return "nested_demo"; }
            @Override public AttachInfo attach(byte[] id, CatalogAttachRequest request) {
                return new AttachInfo(null, "Nested catalog", Map.of(), true, false);
            }
            @Override public long version(byte[] id) { return 1; }
            @Override public List<SchemaInfo> schemas(byte[] id) {
                return List.of(new SchemaInfo(null, Map.of(), List.of("nested"), null),
                        new SchemaInfo(null, Map.of(), List.of("nested", "deep"), null));
            }
            @Override public List<FunctionInfo> functions(byte[] id, List<String> path) {
                return path.equals(List.of("nested", "deep"))
                        ? List.of(CatalogInterface.functionInfoFor(nested, path, "public")) : List.of();
            }
        }).registerExtraCatalogTableFunction("nested_demo", List.of("nested", "deep"), nested);
        worker.runFromArgs(args);
    }

    public static final class Sequence extends CountdownTableFunction {
        private final String name;
        private final boolean projection;
        Sequence(String name, boolean projection) { this.name = name; this.projection = projection; }
        @Override public String name() { return name; }
        @Override protected Schema outputSchema() { return COLUMNS; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Deterministic multibatch fixture").withPushdown(projection, false, false);
        }
        @Override protected long defaultBatchSize() { return 3; }
        @Override public TableProducerState createProducer(TableInitParams params) {
            long count = ((Number) params.arguments().positionalAt(0)).longValue();
            if (count < 0) throw new IllegalArgumentException("count must be nonnegative");
            Number size = (Number) params.arguments().named().get("batch_size");
            long batchSize = size == null ? 3 : size.longValue();
            if (batchSize < 1 || batchSize > 10_000) throw new IllegalArgumentException("invalid batch_size");
            SequenceState state = new SequenceState();
            state.count = count;
            state.batchSize = batchSize;
            state.schemaJson = params.outputSchema().toJson();
            return state;
        }
    }

    public static final class SequenceState extends TableProducerState {
        public long count;
        public long next;
        public long batchSize;
        public String schemaJson;
        public boolean emptyEmitted;
        public SequenceState() {}
        @Override public void produceTick(OutputCollector out, CallContext context) {
            if (next >= count && emptyEmitted) { out.finish(); return; }
            try {
                VectorSchemaRoot root = VectorSchemaRoot.create(Schema.fromJSON(schemaJson), Allocators.root());
                root.allocateNew();
                // A legal empty batch before the first row exercises the consumer's EOS distinction.
                int size = emptyEmitted ? (int) Math.min(batchSize, count - next) : 0;
                emptyEmitted = true;
                for (int row = 0; row < size; row++) {
                    long value = next + row;
                    for (FieldVector vector : root.getFieldVectors()) {
                        switch (vector.getName()) {
                            case "id" -> ((BigIntVector) vector).setSafe(row, value);
                            case "label" -> {
                                if (value % 3 == 0) vector.setNull(row);
                                else ((VarCharVector) vector).setSafe(row, ("row-" + value).getBytes(StandardCharsets.UTF_8));
                            }
                            case "active" -> ((BitVector) vector).setSafe(row, value % 2 == 0 ? 1 : 0);
                            case "amount" -> ((DecimalVector) vector).setSafe(row, BigDecimal.valueOf(value * 125, 2));
                            case "payload" -> ((VarBinaryVector) vector).setSafe(row, new byte[] {(byte) value, 42});
                            case "stamp" -> ((TimeStampMicroVector) vector).setSafe(row, value * 1_000_000 - 1);
                            default -> throw new IllegalStateException(vector.getName());
                        }
                    }
                }
                next += size;
                root.setRowCount(size);
                out.emit(root);
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public static final class Echo extends SimpleTableFunction {
        @Override public String name() { return "echo"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("Nullable argument echo"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("a", 0, Schemas.INT64, true),
                    new ArgSpec("b", 1, Schemas.INT64, true), ArgSpec.named("c", Schemas.INT64, "99"));
        }
        @Override protected Schema outputSchema() {
            return Schemas.of(Schemas.nullable("a", Schemas.INT64), Schemas.nullable("b", Schemas.INT64),
                    Schemas.nullable("c", Schemas.INT64));
        }
        @Override public TableProducerState createProducer(TableInitParams params) {
            EchoState state = new EchoState();
            state.a = (Long) params.arguments().positionalAt(0);
            state.b = (Long) params.arguments().positionalAt(1);
            state.c = params.arguments().named().containsKey("c") ? (Long) params.arguments().named().get("c") : Long.valueOf(99);
            return state;
        }
    }
    public static final class EchoState extends TableProducerState {
        public Long a;
        public Long b;
        public Long c;
        public boolean done;
        public EchoState() {}
        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            var root = VectorSchemaRoot.create(new Echo().outputSchema(), Allocators.root());
            root.allocateNew();
            Long[] values = {a, b, c};
            for (int i = 0; i < 3; i++) {
                var vector = (BigIntVector) root.getVector(i);
                if (values[i] == null) vector.setNull(0); else vector.setSafe(0, values[i]);
            }
            root.setRowCount(1);
            out.emit(root);
        }
    }
}
