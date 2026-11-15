package farm.query.vgicalcite;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/** Decimal128 precision and fractional averages; use {@link VgiDriver} for automatic aggregate widening. */
public final class VgiTypeSystem extends RelDataTypeSystemImpl {
    public static final VgiTypeSystem INSTANCE = new VgiTypeSystem();
    @Override public int getMaxScale(SqlTypeName typeName) {
        return typeName == SqlTypeName.DECIMAL ? 38 : super.getMaxScale(typeName);
    }
    @Override public int getMaxPrecision(SqlTypeName typeName) {
        return typeName == SqlTypeName.DECIMAL ? 38 : super.getMaxPrecision(typeName);
    }

    @Override public RelDataType deriveSumType(RelDataTypeFactory factory, RelDataType argument) {
        // Calcite otherwise keeps a small integer's accumulator at its input
        // width. BIGINT can hold every sum of two int32 values. Larger totals
        // need DECIMAL inputs, inserted automatically by VgiDriver's validator.
        if (SqlTypeUtil.isIntType(argument)) {
            return factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT),
                    argument.isNullable());
        }
        return super.deriveSumType(factory, argument);
    }

    @Override public RelDataType deriveAvgAggType(RelDataTypeFactory factory, RelDataType argument) {
        if (SqlTypeUtil.isExactNumeric(argument)) {
            return factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DOUBLE),
                    argument.isNullable());
        }
        return super.deriveAvgAggType(factory, argument);
    }
}
