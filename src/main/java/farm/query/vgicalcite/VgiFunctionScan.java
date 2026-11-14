package farm.query.vgicalcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.AbstractRelNode;
import java.util.List;

/** Keeps the actual bound macro instance in the prepared query's own runtime context. */
final class VgiFunctionScan extends AbstractRelNode implements EnumerableRel {
    private final RelOptTable table;
    private final VgiTable source;

    VgiFunctionScan(RelOptCluster cluster, RelOptTable table, VgiTable source) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE));
        this.table = table;
        this.source = source;
    }

    @Override protected org.apache.calcite.rel.type.RelDataType deriveRowType() { return table.getRowType(); }

    @Override public double estimateRowCount(org.apache.calcite.rel.metadata.RelMetadataQuery metadata) {
        return table.getRowCount();
    }

    @Override public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
        if (!inputs.isEmpty()) throw new IllegalArgumentException("A VGI scan has no inputs");
        return new VgiFunctionScan(getCluster(), table, source);
    }

    @Override public Result implement(EnumerableRelImplementor implementor, Prefer prefer) {
        // Never look the function up as a named table: that loses its bound arguments.
        var expression = Expressions.call(implementor.stash(source, VgiTable.class), "scanAll", DataContext.ROOT);
        var physical = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
        return implementor.result(physical, Blocks.toBlock(expression));
    }

    @Override public org.apache.calcite.rel.RelWriter explainTerms(org.apache.calcite.rel.RelWriter writer) {
        // Otherwise sequence(3) and sequence(5) have identical TableScan digests and
        // Calcite is entitled to merge them during common-subexpression elimination.
        return super.explainTerms(writer).item("function", table.getQualifiedName()).item("arguments", source.invocationKey());
    }
}
