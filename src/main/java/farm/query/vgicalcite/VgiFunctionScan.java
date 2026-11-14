package farm.query.vgicalcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rex.RexNode;
import java.util.ArrayList;
import java.util.List;

/** Keeps the actual bound macro instance in the prepared query's own runtime context. */
final class VgiFunctionScan extends AbstractRelNode implements EnumerableRel {
    private final RelOptTable table;
    private final VgiTable source;
    private final List<RexNode> filters;

    VgiFunctionScan(RelOptCluster cluster, RelOptTable table, VgiTable source) {
        this(cluster, table, source, List.of());
    }

    private VgiFunctionScan(RelOptCluster cluster, RelOptTable table, VgiTable source, List<RexNode> filters) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE));
        this.table = table;
        this.source = source;
        this.filters = List.copyOf(filters);
    }

    @Override protected org.apache.calcite.rel.type.RelDataType deriveRowType() { return table.getRowType(); }

    @Override public double estimateRowCount(org.apache.calcite.rel.metadata.RelMetadataQuery metadata) {
        return table.getRowCount() * (filters.isEmpty() ? 1 : 0.5);
    }

    @Override public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
        if (!inputs.isEmpty()) throw new IllegalArgumentException("A VGI scan has no inputs");
        return new VgiFunctionScan(getCluster(), table, source, filters);
    }

    @Override public Result implement(EnumerableRelImplementor implementor, Prefer prefer) {
        // Never look the function up as a named table: that loses its bound arguments.
        var expression = Expressions.call(implementor.stash(source, VgiTable.class), "scanFiltered",
                DataContext.ROOT, implementor.stash(filters, List.class));
        var physical = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
        return implementor.result(physical, Blocks.toBlock(expression));
    }

    @Override public org.apache.calcite.rel.RelWriter explainTerms(org.apache.calcite.rel.RelWriter writer) {
        // Otherwise sequence(3) and sequence(5) have identical TableScan digests and
        // Calcite is entitled to merge them during common-subexpression elimination.
        return super.explainTerms(writer).item("function", table.getQualifiedName()).item("arguments", source.invocationKey())
                .itemIf("filters", filters, !filters.isEmpty());
    }

    static void registerRules(RelOptPlanner planner) {
        planner.addRule(FILTER_RULE);
        planner.addRule(CALC_RULE);
    }

    // Preserve the full Calcite condition above the scan. These rules add pruning
    // hints without claiming exact worker evaluation or losing invocation identity.
    private static final RelOptRule FILTER_RULE = new RelOptRule(
            RelOptRule.operand(Filter.class, RelOptRule.operand(VgiFunctionScan.class, RelOptRule.none())),
            "VgiFunctionFilterPushdown") {
        @Override public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            VgiFunctionScan scan = call.rel(1);
            VgiFunctionScan replacement = scan.withFilters(filter.getCondition());
            if (replacement != null) call.transformTo(filter.copy(filter.getTraitSet(), replacement, filter.getCondition()));
        }
    };

    private static final RelOptRule CALC_RULE = new RelOptRule(
            RelOptRule.operand(Calc.class, RelOptRule.operand(VgiFunctionScan.class, RelOptRule.none())),
            "VgiFunctionCalcPushdown") {
        @Override public void onMatch(RelOptRuleCall call) {
            Calc calc = call.rel(0);
            if (calc.getProgram().getCondition() == null) return;
            VgiFunctionScan scan = call.rel(1);
            VgiFunctionScan replacement = scan.withFilters(calc.getProgram().expandLocalRef(calc.getProgram().getCondition()));
            if (replacement != null) call.transformTo(calc.copy(calc.getTraitSet(), replacement, calc.getProgram()));
        }
    };

    private VgiFunctionScan withFilters(RexNode condition) {
        List<RexNode> combined = new ArrayList<>(filters);
        for (RexNode filter : RelOptUtil.conjunctions(condition)) {
            if (!combined.contains(filter)) combined.add(filter);
        }
        return combined.size() == filters.size() ? null : new VgiFunctionScan(getCluster(), table, source, combined);
    }
}
