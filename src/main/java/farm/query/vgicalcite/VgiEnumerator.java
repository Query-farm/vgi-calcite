package farm.query.vgicalcite;

import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Retains only the current Arrow batch and copies each returned row's Java values. */
final class VgiEnumerator implements Enumerator<Object[]> {
    private final VgiTable table;
    private final int[] projects;
    private final DataContext context;
    private final List<RexNode> filters;
    private final AtomicBoolean cancelled;
    private final org.apache.calcite.rel.type.RelDataTypeFactory typeFactory;
    private VgiSession session;
    private Iterator<VgiTable.Invocation> branches;
    private RpcStream<? extends StreamState> stream;
    private VectorSchemaRoot batch;
    private org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries;
    private int[] sourceColumns;
    private int row;
    private Object[] current;
    private boolean closed;
    private boolean failed;

    VgiEnumerator(VgiTable table, DataContext context, int[] projects, List<RexNode> filters) {
        this.table = table;
        this.projects = projects;
        this.context = context;
        this.filters = filters;
        this.cancelled = DataContext.Variable.CANCEL_FLAG.get(context);
        this.typeFactory = context.getTypeFactory();
    }

    @Override public Object[] current() {
        if (current == null) throw new NoSuchElementException("Enumerator is not on a row");
        return current;
    }

    @Override public boolean moveNext() {
        if (closed) return false;
        current = null;
        try {
            while (true) {
                if (cancelled != null && cancelled.get() || Thread.currentThread().isInterrupted()) {
                    throw new CancellationException("VGI scan cancelled");
                }
                if (session == null) {
                    session = VgiSession.open(table.config);
                    branches = table.resolve(session, VgiFilters.translate(filters, table.columns, context)).iterator();
                }
                if (batch != null && row < batch.getRowCount()) {
                    Object[] values = new Object[projects.length];
                    for (int i = 0; i < values.length; i++) values[i] = ArrowTypes.read(batch.getVector(sourceColumns[i]), row, dictionaries);
                    row++;
                    current = values;
                    return true;
                }
                if (stream == null) {
                    if (!branches.hasNext()) {
                        session.detach();
                        close();
                        return false;
                    }
                    start(branches.next());
                }
                try {
                    var next = stream.tick();
                    batch = next.root();
                    dictionaries = next.dictionaryProvider();
                    row = 0;
                    if (batch.getFieldVectors().size() != expectedColumnCount) {
                        throw new IllegalStateException("VGI stream returned a different column count than its binding");
                    }
                } catch (NoSuchElementException end) {
                    stream.close();
                    stream = null;
                    batch = null;
                }
            }
        } catch (RuntimeException | Error e) {
            failed = true;
            try { close(); } catch (RuntimeException closing) { e.addSuppressed(closing); }
            throw e;
        }
    }

    private int expectedColumnCount;

    private void start(VgiTable.Invocation invocation) {
        var bound = VgiTable.bind(session, invocation);
        Schema actual = SchemaUtil.deserializeSchema(bound.output_schema());
        if (actual.getFields().size() != table.columns.getFields().size()) {
            throw new IllegalStateException("VGI output schema changed since planning; reconnect to refresh metadata");
        }
        // Catalog columns may rename scan-function columns, but types and order must agree.
        var factory = typeFactory;
        for (int i = 0; i < actual.getFields().size(); i++) {
            var expected = ArrowTypes.type(factory, table.columns.getFields().get(i));
            var received = ArrowTypes.type(factory, actual.getFields().get(i));
            if (!factory.createTypeWithNullability(expected, true).equals(factory.createTypeWithNullability(received, true))
                    || !expected.isNullable() && received.isNullable()) {
                throw new IllegalStateException("VGI output type changed at column " + i + "; reconnect to refresh metadata");
            }
        }
        // Encode against the full bind schema: catalog aliases and projected positions
        // are not the function's wire column identities.
        VgiFilters translated = VgiFilters.translate(filters, actual, context);
        table.requireFilterDelivery(invocation, translated);
        byte[] pushedFilters = invocation.filtering() ? translated.encode() : null;
        // Empty projection (COUNT(*)) still needs rows. Some workers interpret [] as no rows.
        List<Integer> projection = invocation.projection() && projects.length > 0
                ? Arrays.stream(projects).distinct().sorted().boxed().toList() : null;
        expectedColumnCount = projection == null ? actual.getFields().size() : projection.size();
        sourceColumns = new int[projects.length];
        for (int i = 0; i < projects.length; i++) sourceColumns[i] = projection == null ? projects[i] : projection.indexOf(projects[i]);
        stream = session.service.init(new InitRequest(RecordCodec.serializeToBytes(VgiTable.request(session, invocation)),
                bound.output_schema(), bound.opaque_data(), projection, pushedFilters, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null), null);
    }

    @Override public void reset() { throw new UnsupportedOperationException("Reopen a VGI enumerator to rescan"); }

    @Override public void close() {
        if (closed) return;
        closed = true;
        current = null;
        batch = null;
        try {
            // Cancellation is protocol-aware: stop producing rather than drain the full relation.
            if (stream != null) stream.cancel();
            if (!failed && session != null) session.detach();
        } finally {
            stream = null;
            if (session != null) session.close();
        }
    }
}
