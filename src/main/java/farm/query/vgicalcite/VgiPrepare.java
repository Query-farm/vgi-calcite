package farm.query.vgicalcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.validate.SqlValidator;

import java.lang.reflect.Type;
import java.util.function.UnaryOperator;

/** Installs aggregate validation on this connection, including expanded views. */
final class VgiPrepare extends CalcitePrepareImpl {
    @Override protected CalcitePreparingStmt getPreparingStmt(Context context, Type elementType,
            CalciteCatalogReader catalogReader, RelOptPlanner planner) {
        var typeFactory = context.getTypeFactory();
        var reader = new VgiCatalogReader(context.getRootSchema(), catalogReader.getSchemaPaths().getFirst(),
                typeFactory, context.config());
        var prefer = elementType == Object[].class ? EnumerableRel.Prefer.ARRAY : EnumerableRel.Prefer.CUSTOM;
        return new CalcitePreparingStmt(this, context, reader, typeFactory,
                context.getRootSchema(), prefer, createCluster(planner, new RexBuilder(typeFactory)),
                enableBindable ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE,
                createConvertletTable()) {
            @Override protected SqlValidator createSqlValidator(CatalogReader reader,
                    UnaryOperator<SqlValidator.Config> configTransform) {
                var catalog = (CalciteCatalogReader) reader;
                var vgiReader = catalog instanceof VgiCatalogReader ? catalog
                        : new VgiCatalogReader(context.getRootSchema(), catalog.getSchemaPaths().getFirst(),
                                context.getTypeFactory(), context.config());
                SqlValidator standard = super.createSqlValidator(vgiReader, configTransform);
                return new VgiSqlValidator(standard.getOperatorTable(), vgiReader,
                        context.getTypeFactory(), standard.config());
            }
        };
    }
}
