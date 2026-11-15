package farm.query.vgicalcite;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

/** Derives model-view result types using the querying connection's aggregate policy. */
final class VgiCatalogReader extends CalciteCatalogReader {
    VgiCatalogReader(CalciteSchema root, List<String> path, JavaTypeFactory types,
            CalciteConnectionConfig config) {
        super(root, path, types, config);
    }

    @Override public VgiCatalogReader withSchemaPath(List<String> path) {
        return new VgiCatalogReader(rootSchema, path, (JavaTypeFactory) typeFactory, config);
    }

    @Override public Prepare.PreparingTable getTable(List<String> names) {
        var table = super.getTable(names);
        if (!(table instanceof RelOptTableImpl relTable)) return table;
        ViewTable view = table.unwrap(ViewTable.class);
        if (view == null) return table;

        // Calcite's ViewTableMacro analyzes views on a shared stock connection.
        // Its row type can otherwise force an expanded decimal SUM back to BIGINT.
        var reader = withSchemaPath(view.getSchemaPath());
        var operators = SqlOperatorTables.chain(
                config.fun(SqlOperatorTable.class, SqlStdOperatorTable.instance()), reader);
        var validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true)
                .withEmbeddedQuery(true);
        var validator = new VgiSqlValidator(operators, reader, (JavaTypeFactory) typeFactory, validatorConfig);
        try {
            // Match CalcitePreparingStmt.expandView's parser configuration.
            var node = validator.validate(SqlParser.create(view.getViewSql()).parseQuery());
            return relTable.copy(validator.getValidatedNodeType(node));
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("Cannot parse view " + names, e);
        }
    }
}
