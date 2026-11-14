package farm.query.vgicalcite;

import farm.query.vgi.client.FilterPredicate;
import farm.query.vgi.client.ProjectedColumn;
import farm.query.vgi.client.PushdownFiltersEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.pushdown.ComparisonOperator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.DataContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * Translates the subset of Calcite predicates equivalent under VGI's standard
 * semantics. All originals remain in Calcite: worker filters are pruning hints.
 * In particular, rechecking cannot repair an overly restrictive translation.
 */
final class VgiFilters {
    static final String ENCODING = "vgi.filters.v2";
    static final String PROFILE = "vgi.duckdb.standard.v1";
    private record ColumnFilter(int column, FilterPredicate predicate) {}
    private final Schema schema;
    private final List<ColumnFilter> filters;

    private VgiFilters(Schema schema, List<ColumnFilter> filters) {
        this.schema = schema;
        this.filters = List.copyOf(filters);
    }

    static boolean supported(FunctionInfo function) {
        return function != null && Boolean.TRUE.equals(function.filter_pushdown())
                && function.filter_semantic_profiles().contains(PROFILE);
    }

    static VgiFilters translate(List<RexNode> expressions, Schema schema, DataContext context) {
        List<ColumnFilter> filters = new ArrayList<>();
        RexBuilder builder = new RexBuilder(context.getTypeFactory());
        for (RexNode expression : expressions) {
            // Calcite represents IN lists, BETWEEN, and merged ranges as SEARCH.
            RexNode expanded = RexUtil.expandSearch(builder, null, expression);
            for (RexNode conjunct : RelOptUtil.conjunctions(expanded)) {
                ColumnFilter filter = translate(conjunct, schema, context);
                if (filter != null) filters.add(filter);
            }
        }
        return new VgiFilters(schema, filters);
    }

    boolean covers(List<String> path) {
        return path.size() == 1 && filters.stream()
                .anyMatch(f -> schema.getFields().get(f.column()).getName().equals(path.getFirst()));
    }

    boolean coversIndex(int index) {
        return filters.stream().anyMatch(f -> f.column() == index);
    }

    byte[] encode() {
        if (filters.isEmpty()) return null;
        var encoder = PushdownFiltersEncoder.builder();
        for (ColumnFilter filter : filters) {
            encoder.filter(ProjectedColumn.of(schema.getFields().get(filter.column()).getName(), filter.column()),
                    filter.predicate());
        }
        return encoder.encode().pushdownFilters();
    }

    private static ColumnFilter translate(RexNode node, Schema schema, DataContext context) {
        if (node instanceof RexInputRef ref && ref.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            return column(ref, FilterPredicate.eq(true), schema);
        }
        if (!(node instanceof RexCall call)) return null;
        List<RexNode> args = call.getOperands();
        if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR) {
            List<ColumnFilter> children = args.stream().map(a -> translate(a, schema, context)).toList();
            if (children.isEmpty() || children.stream().anyMatch(c -> c == null
                    || c.column() != children.getFirst().column())) return null;
            FilterPredicate[] predicates = children.stream().map(ColumnFilter::predicate).toArray(FilterPredicate[]::new);
            return new ColumnFilter(children.getFirst().column(), call.getKind() == SqlKind.AND
                    ? FilterPredicate.and(predicates) : FilterPredicate.or(predicates));
        }
        if (args.size() == 1 && inputRef(args.getFirst()) instanceof RexInputRef ref) {
            FilterPredicate predicate = switch (call.getKind()) {
                case IS_NULL -> FilterPredicate.isNull();
                case IS_NOT_NULL -> FilterPredicate.isNotNull();
                case IS_TRUE -> FilterPredicate.eq(true);
                case IS_FALSE, NOT -> ref.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
                        ? FilterPredicate.eq(false) : null;
                case IS_NOT_TRUE -> FilterPredicate.or(FilterPredicate.eq(false), FilterPredicate.isNull());
                case IS_NOT_FALSE -> FilterPredicate.or(FilterPredicate.eq(true), FilterPredicate.isNull());
                default -> null;
            };
            return predicate == null ? null : column(ref, predicate, schema);
        }
        ComparisonOperator op = switch (call.getKind()) {
            case EQUALS -> ComparisonOperator.EQ;
            case NOT_EQUALS -> ComparisonOperator.NE;
            case LESS_THAN -> ComparisonOperator.LT;
            case LESS_THAN_OR_EQUAL -> ComparisonOperator.LE;
            case GREATER_THAN -> ComparisonOperator.GT;
            case GREATER_THAN_OR_EQUAL -> ComparisonOperator.GE;
            default -> null;
        };
        if (op == null || args.size() != 2) return null;
        RexNode left = args.get(0), right = args.get(1);
        if (inputRef(left) == null && inputRef(right) != null) {
            RexNode swap = left; left = right; right = swap;
            op = switch (op) {
                case LT -> ComparisonOperator.GT;
                case LE -> ComparisonOperator.GE;
                case GT -> ComparisonOperator.LT;
                case GE -> ComparisonOperator.LE;
                default -> op;
            };
        }
        RexInputRef ref = inputRef(left);
        if (ref == null || !eligible(ref, schema)) return null;
        Object value;
        if (right instanceof RexLiteral literal) {
            if (literal.isNull()) return null;
            value = literal.getValue3();
        } else if (right instanceof RexDynamicParam parameter) {
            // Resolve for each execution, never cache a prepared statement's parameter values.
            value = context.get("?" + parameter.getIndex());
        } else return null;
        if (value == null) return null;
        try {
            FilterPredicate predicate = comparison(ref, schema.getFields().get(ref.getIndex()).getType(), op, value);
            return predicate == null ? null : new ColumnFilter(ref.getIndex(), predicate);
        } catch (ArithmeticException | IllegalArgumentException unsupportedLiteral) {
            // Out-of-range or inexact constants must not be truncated into a different predicate.
            return null;
        }
    }

    private static ColumnFilter column(RexInputRef ref, FilterPredicate predicate, Schema schema) {
        return eligible(ref, schema) ? new ColumnFilter(ref.getIndex(), predicate) : null;
    }

    /** Calcite inserts widening casts for small integers and mixed decimal scales. */
    private static RexInputRef inputRef(RexNode node) {
        while (node instanceof RexCall call && call.getKind() == SqlKind.CAST) {
            RexNode input = call.getOperands().getFirst();
            var source = input.getType();
            var target = node.getType();
            int sourceDigits = integerDigits(source.getSqlTypeName());
            int targetDigits = integerDigits(target.getSqlTypeName());
            boolean wideningInteger = sourceDigits > 0 && targetDigits >= sourceDigits;
            boolean wideningDecimal = target.getSqlTypeName() == SqlTypeName.DECIMAL
                    && (sourceDigits > 0 && target.getPrecision() - target.getScale() >= sourceDigits
                    || source.getSqlTypeName() == SqlTypeName.DECIMAL
                    && target.getScale() >= source.getScale()
                    && target.getPrecision() - target.getScale() >= source.getPrecision() - source.getScale());
            if (!wideningInteger && !wideningDecimal) return null;
            node = input;
        }
        return node instanceof RexInputRef ref ? ref : null;
    }

    private static int integerDigits(SqlTypeName type) {
        return switch (type) {
            case TINYINT -> 3;
            case SMALLINT -> 5;
            case INTEGER -> 10;
            case BIGINT -> 19;
            default -> 0;
        };
    }

    private static boolean eligible(RexInputRef ref, Schema schema) {
        Field field = schema.getFields().get(ref.getIndex());
        if (field.getDictionary() != null || field.getMetadata().containsKey("ARROW:extension:name")) return false;
        return switch (field.getType().getTypeID()) {
            case Int, Decimal, Bool, Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary,
                 Date, Time, Timestamp, FloatingPoint -> true;
            default -> false;
        };
    }

    private static FilterPredicate comparison(RexInputRef ref, ArrowType type, ComparisonOperator op, Object value) {
        Object scalar;
        switch (type.getTypeID()) {
            case Int -> {
                var integer = (ArrowType.Int) type;
                BigInteger n = decimal(value).toBigIntegerExact();
                int bits = integer.getBitWidth();
                BigInteger min = integer.getIsSigned() ? BigInteger.ONE.shiftLeft(bits - 1).negate() : BigInteger.ZERO;
                BigInteger max = BigInteger.ONE.shiftLeft(integer.getIsSigned() ? bits - 1 : bits).subtract(BigInteger.ONE);
                if (n.compareTo(min) < 0 || n.compareTo(max) > 0) return null;
                scalar = n;
            }
            case Decimal -> {
                var d = (ArrowType.Decimal) type;
                BigDecimal n = decimal(value).setScale(d.getScale(), RoundingMode.UNNECESSARY);
                if (d.getBitWidth() != 128 || n.precision() > d.getPrecision()) return null;
                scalar = n;
            }
            case Bool -> {
                if (!(value instanceof Boolean) || op != ComparisonOperator.EQ && op != ComparisonOperator.NE) return null;
                scalar = value;
            }
            case Utf8, LargeUtf8 -> {
                // Java UTF-16 and VGI UTF-8 ordering differ for supplementary characters.
                // Equality is equivalent only without a locale-sensitive collation.
                if (op != ComparisonOperator.EQ && op != ComparisonOperator.NE
                        || ref.getType().getCollation() != null && ref.getType().getCollation().getCollator() != null) return null;
                if (value instanceof org.apache.calcite.util.NlsString s) scalar = s.getValue();
                else if (value instanceof String) scalar = value;
                else return null;
            }
            case Binary, LargeBinary, FixedSizeBinary -> {
                if (op != ComparisonOperator.EQ && op != ComparisonOperator.NE) return null;
                if (value instanceof org.apache.calcite.avatica.util.ByteString bytes) scalar = bytes.getBytes();
                else if (value instanceof byte[]) scalar = value;
                else return null;
                if (type instanceof ArrowType.FixedSizeBinary fixed && ((byte[]) scalar).length != fixed.getByteWidth()) return null;
            }
            case Date -> {
                if (!(value instanceof Number)) return null;
                long days = decimal(value).longValueExact();
                if (((ArrowType.Date) type).getUnit() == DateUnit.MILLISECOND) {
                    return floored(type, op, days, 86_400_000L);
                }
                scalar = Math.toIntExact(days);
            }
            case Time, Timestamp -> {
                if (!(value instanceof Number)) return null;
                TimeUnit unit;
                if (type instanceof ArrowType.Timestamp timestamp) {
                    if (timestamp.getTimezone() != null) return null;
                    unit = timestamp.getUnit();
                } else unit = ((ArrowType.Time) type).getUnit();
                long millis = decimal(value).longValueExact();
                if (unit == TimeUnit.MICROSECOND) return floored(type, op, millis, 1_000L);
                if (unit == TimeUnit.NANOSECOND) return floored(type, op, millis, 1_000_000L);
                if (unit == TimeUnit.SECOND) {
                    if (millis % 1_000 != 0) return null;
                    scalar = millis / 1_000;
                } else scalar = millis;
            }
            // NaN and signed-zero behavior needs a separate equivalence contract.
            default -> { return null; }
        }
        return FilterPredicate.compare(op, ScalarValue.of(type, scalar));
    }

    private static BigDecimal decimal(Object value) {
        if (!(value instanceof Number)) throw new IllegalArgumentException("Not numeric");
        return value instanceof BigDecimal d ? d : new BigDecimal(value.toString());
    }

    /** Invert floor(raw / factor) without losing sub-millisecond or pre-epoch values. */
    private static FilterPredicate floored(ArrowType type, ComparisonOperator op, long value, long factor) {
        long lower = Math.multiplyExact(value, factor);
        if (op == ComparisonOperator.GE || op == ComparisonOperator.LT) {
            return FilterPredicate.compare(op, ScalarValue.of(type, lower));
        }
        long upper = Math.multiplyExact(Math.addExact(value, 1), factor);
        return switch (op) {
            case EQ -> FilterPredicate.and(FilterPredicate.ge(ScalarValue.of(type, lower)),
                    FilterPredicate.lt(ScalarValue.of(type, upper)));
            case NE -> FilterPredicate.or(FilterPredicate.lt(ScalarValue.of(type, lower)),
                    FilterPredicate.ge(ScalarValue.of(type, upper)));
            case GT -> FilterPredicate.ge(ScalarValue.of(type, upper));
            case LE -> FilterPredicate.lt(ScalarValue.of(type, upper));
            default -> throw new IllegalArgumentException("Unexpected comparison");
        };
    }
}
