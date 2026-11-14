package farm.query.vgicalcite;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/** Opt-in DECIMAL(38) support for Arrow decimal128 and uint64 columns. */
public final class VgiTypeSystem extends RelDataTypeSystemImpl {
    public static final VgiTypeSystem INSTANCE = new VgiTypeSystem();
    @Override public int getMaxScale(SqlTypeName typeName) {
        return typeName == SqlTypeName.DECIMAL ? 38 : super.getMaxScale(typeName);
    }
    @Override public int getMaxPrecision(SqlTypeName typeName) {
        return typeName == SqlTypeName.DECIMAL ? 38 : super.getMaxPrecision(typeName);
    }
}
