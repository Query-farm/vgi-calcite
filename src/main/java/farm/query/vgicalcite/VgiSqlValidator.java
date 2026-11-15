package farm.query.vgicalcite;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalciteSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.schema.impl.ViewTable;

/** Promotes integer aggregate operands before return types and accumulators are derived. */
final class VgiSqlValidator extends CalciteSqlValidator {
    VgiSqlValidator(SqlOperatorTable operators, CalciteCatalogReader catalogReader,
            JavaTypeFactory typeFactory, Config config) {
        super(operators, catalogReader, typeFactory, config);
    }

    @Override protected void validateNamespace(SqlValidatorNamespace namespace, RelDataType targetRowType) {
        super.validateNamespace(namespace, targetRowType);
        // Calcite's EmptyScope constructs table namespaces directly from schema
        // entries, bypassing CatalogReader.getTable. Update that underlying
        // namespace before identifier and alias namespaces derive their types.
        var table = namespace.getTable();
        if (namespace.getNode() == null && table != null && table.unwrap(ViewTable.class) != null) {
            var validatedTable = getCatalogReader().getTable(table.getQualifiedName());
            if (validatedTable != null) namespace.setType(validatedTable.getRowType());
        }
    }

    @Override public RelDataType deriveType(SqlValidatorScope scope, SqlNode expression) {
        if (expression instanceof SqlCall call && call.operandCount() == 1
                && (call.getOperator() == SqlStdOperatorTable.SUM
                    || call.getOperator() == SqlStdOperatorTable.AVG)) {
            SqlNode operand = call.operand(0);
            RelDataType inputType = super.deriveType(scope.getOperandScope(call), operand);
            if (SqlTypeUtil.isIntType(inputType)) {
                // Widen the input, not merely the result: Calcite's decimal SUM implementor
                // expects BigDecimal operands, and AVG is reduced to SUM / COUNT.
                var position = operand.getParserPosition();
                var decimal = new SqlDataTypeSpec(
                        new SqlBasicTypeNameSpec(SqlTypeName.DECIMAL, 38, 0, position), position);
                call.setOperand(0, SqlStdOperatorTable.CAST.createCall(position, operand, decimal));
            }
        }
        return super.deriveType(scope, expression);
    }
}
