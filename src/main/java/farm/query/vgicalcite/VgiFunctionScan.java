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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import java.util.ArrayList;
import java.util.List;

/** Keeps the actual bound macro instance in the prepared query's own runtime context. */
final class VgiFunctionScan extends AbstractRelNode implements EnumerableRel {
    private final RelOptTable table;
    private final VgiTable source;
    private final List<RexNode> filters;
    // Ordinals in the original bind schema. Filters always use that schema too.
    private final List<Integer> projects;

    VgiFunctionScan(RelOptCluster cluster, RelOptTable table, VgiTable source) {
        this(cluster, table, source, List.of(),
                java.util.stream.IntStream.range(0, table.getRowType().getFieldCount()).boxed().toList());
    }

    private VgiFunctionScan(RelOptCluster cluster, RelOptTable table, VgiTable source,
                            List<RexNode> filters, List<Integer> projects) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE));
        this.table = table;
        this.source = source;
        this.filters = List.copyOf(filters);
        this.projects = List.copyOf(projects);
    }

    @Override protected org.apache.calcite.rel.type.RelDataType deriveRowType() {
        var builder = getCluster().getTypeFactory().builder();
        for (int ordinal : projects) builder.add(table.getRowType().getFieldList().get(ordinal));
        return builder.build();
    }

    @Override public double estimateRowCount(org.apache.calcite.rel.metadata.RelMetadataQuery metadata) {
        return table.getRowCount() * (filters.isEmpty() ? 1 : 0.5);
    }

    @Override public org.apache.calcite.plan.RelOptCost computeSelfCost(RelOptPlanner planner,
            org.apache.calcite.rel.metadata.RelMetadataQuery metadata) {
        double rows = metadata.getRowCount(this);
        return planner.getCostFactory().makeCost(rows, rows * (projects.size() + 1), 0);
    }

    @Override public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
        if (!inputs.isEmpty()) throw new IllegalArgumentException("A VGI scan has no inputs");
        return new VgiFunctionScan(getCluster(), table, source, filters, projects);
    }

    @Override public Result implement(EnumerableRelImplementor implementor, Prefer prefer) {
        // Never look the function up as a named table: that loses its bound arguments.
        var expression = Expressions.call(implementor.stash(source, VgiTable.class), "scanProjected",
                DataContext.ROOT, implementor.stash(filters, List.class),
                implementor.stash(projects.stream().mapToInt(Integer::intValue).toArray(), int[].class));
        var sourcePhysical = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
        var physical = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY);
        // The scanner always emits Object[], but Calcite optimizes one-column
        // inputs to scalars (and zero-column inputs to empty lists). Convert the
        // actual rows as well as describing their format to parent operators.
        return implementor.result(physical, Blocks.toBlock(sourcePhysical.convertTo(expression, physical)));
    }

    @Override public org.apache.calcite.rel.RelWriter explainTerms(org.apache.calcite.rel.RelWriter writer) {
        // Otherwise sequence(3) and sequence(5) have identical TableScan digests and
        // Calcite is entitled to merge them during common-subexpression elimination.
        return super.explainTerms(writer).item("function", table.getQualifiedName()).item("arguments", source.invocationKey())
                .item("projects", projects).itemIf("filters", filters, !filters.isEmpty());
    }

    static void registerRules(RelOptPlanner planner) {
        planner.addRule(FILTER_RULE);
        planner.addRule(CALC_RULE);
        planner.addRule(PROJECT_RULE);
        planner.addRule(PROJECT_FILTER_RULE);
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
            if (replacement != scan) call.transformTo(filter.copy(filter.getTraitSet(), replacement, filter.getCondition()));
        }
    };

    private static final RelOptRule CALC_RULE = new RelOptRule(
            RelOptRule.operand(Calc.class, RelOptRule.operand(VgiFunctionScan.class, RelOptRule.none())),
            "VgiFunctionCalcPushdown") {
        @Override public void onMatch(RelOptRuleCall call) {
            Calc calc = call.rel(0);
            VgiFunctionScan scan = call.rel(1);
            var program = calc.getProgram();
            RexNode condition = program.getCondition() == null ? null : program.expandLocalRef(program.getCondition());
            List<RexNode> expressions = program.getProjectList().stream().map(program::expandLocalRef).toList();
            VgiFunctionScan filtered = condition == null ? scan : scan.withFilters(condition);
            List<Integer> needed = RelOptUtil.InputFinder.bits(expressions, condition).asList();
            if (needed.size() == scan.projects.size()) {
                if (filtered != scan) call.transformTo(calc.copy(calc.getTraitSet(), filtered, program));
                return;
            }
            VgiFunctionScan replacement = filtered.project(needed);
            RexShuttle remap = remap(needed, replacement.getRowType());
            RexProgram rewritten = RexProgram.create(replacement.getRowType(), remap.apply(expressions),
                    condition == null ? null : condition.accept(remap), calc.getRowType(), scan.getCluster().getRexBuilder());
            call.transformTo(calc.copy(calc.getTraitSet(), replacement, rewritten));
        }
    };

    private static final RelOptRule PROJECT_RULE = new RelOptRule(
            RelOptRule.operand(Project.class, RelOptRule.operand(VgiFunctionScan.class, RelOptRule.none())),
            "VgiFunctionProjectPushdown") {
        @Override public void onMatch(RelOptRuleCall call) {
            Project project = call.rel(0);
            VgiFunctionScan scan = call.rel(1);
            List<Integer> needed = RelOptUtil.InputFinder.bits(project.getProjects(), null).asList();
            if (needed.size() == scan.projects.size()) return;
            VgiFunctionScan replacement = scan.project(needed);
            call.transformTo(project.copy(project.getTraitSet(), replacement,
                    remap(needed, replacement.getRowType()).apply(project.getProjects()), project.getRowType()));
        }
    };

    // Calcite's default JDBC planner can merge Project/Filter into an
    // EnumerableCalc after the main rule program. Prune this form while it is
    // still visible, including every input needed by the retained condition.
    private static final RelOptRule PROJECT_FILTER_RULE = new RelOptRule(
            RelOptRule.operand(Project.class, RelOptRule.operand(Filter.class,
                    RelOptRule.operand(VgiFunctionScan.class, RelOptRule.none()))),
            "VgiFunctionProjectFilterPushdown") {
        @Override public void onMatch(RelOptRuleCall call) {
            Project project = call.rel(0);
            Filter filter = call.rel(1);
            VgiFunctionScan scan = call.rel(2);
            List<Integer> needed = RelOptUtil.InputFinder.bits(project.getProjects(), filter.getCondition()).asList();
            if (needed.size() == scan.projects.size()) return;
            VgiFunctionScan replacement = scan.withFilters(filter.getCondition()).project(needed);
            RexShuttle remap = remap(needed, replacement.getRowType());
            var program = RexProgram.create(replacement.getRowType(), remap.apply(project.getProjects()),
                    filter.getCondition().accept(remap), project.getRowType(), scan.getCluster().getRexBuilder());
            if (program.containsAggs()) return;
            call.transformTo(EnumerableCalc.create(replacement, program));
        }
    };

    private VgiFunctionScan project(List<Integer> ordinals) {
        return new VgiFunctionScan(getCluster(), table, source, filters, ordinals.stream().map(projects::get).toList());
    }

    private static RexShuttle remap(List<Integer> oldOrdinals, org.apache.calcite.rel.type.RelDataType target) {
        return new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef ref) {
                int index = oldOrdinals.indexOf(ref.getIndex());
                if (index < 0) throw new IllegalStateException("VGI projection omitted a referenced column");
                return RexInputRef.of(index, target);
            }
        };
    }

    private VgiFunctionScan withFilters(RexNode condition) {
        RexNode original = condition.accept(new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef ref) {
                return RexInputRef.of(projects.get(ref.getIndex()), table.getRowType());
            }
        });
        List<RexNode> combined = new ArrayList<>(filters);
        for (RexNode filter : RelOptUtil.conjunctions(original)) {
            if (!combined.contains(filter)) combined.add(filter);
        }
        return combined.size() == filters.size() ? this : new VgiFunctionScan(getCluster(), table, source, combined, projects);
    }
}
