package farm.query.vgicalcite;

import farm.query.vgi.CatalogProfiles;
import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VgiCatalogMetadataTest {
    private static ConstraintRecord.Builder constraint(String id) {
        return ConstraintRecord.builder("data", "numbers", id)
                .enforcement(ConstraintRecord.Enforcement.ENFORCED)
                .validation(ConstraintRecord.Validation.ALL_ROWS);
    }

    private static TableInfo table(boolean nullable, ConstraintRecord... constraints) {
        Schema schema = new Schema(List.of(new Field("id", new FieldType(nullable, Schemas.INT64, null), List.of())));
        return new TableInfo(null, Map.of(), "numbers", List.of("data"), SchemaUtil.serializeSchema(schema),
                java.util.Arrays.stream(constraints).map(ConstraintRecord::encode).toList(), null, Map.of(), false,
                null, null, null, null, null, null, null, null, List.of());
    }

    @Test void validatesReadOnlyConstraintsWithoutMutatingSchema() {
        var table = table(false, constraint("nn").notNull("id").build(), constraint("check").check("id >= 0").build());
        assertDoesNotThrow(() -> VgiCatalogMetadata.validate(table, VgiCatalogMetadata.OFFER, null));
        assertFalse(SchemaUtil.deserializeSchema(table.columns()).getFields().getFirst().isNullable());
    }

    @Test void rejectsUnnegotiatedOrEscalatedCapabilities() {
        assertThrows(IllegalStateException.class, () -> VgiCatalogMetadata.validate(
                table(true, constraint("check").check("id >= 0").build()), CatalogProfiles.none(), null));
        for (var selection : List.of(
                new CatalogCapabilities(List.of("unknown.read.v1"), List.of(), List.of(), List.of()),
                new CatalogCapabilities(List.of(CatalogProfiles.SCALAR_EXPRESSION_READ), List.of(), List.of(), List.of()),
                new CatalogCapabilities(VgiCatalogMetadata.OFFER.read(), List.of(), List.of(CatalogProfiles.CONSTRAINT_ENFORCE), List.of()))) {
            assertThrows(IllegalStateException.class, () -> VgiCatalogMetadata.selected(CatalogProfiles.encode(selection)));
        }
        assertEquals(CatalogProfiles.none(), VgiCatalogMetadata.selected(null));
    }

    @Test void rejectsInconsistentNullabilityAndDuplicateIdentities() {
        assertThrows(IllegalStateException.class, () -> VgiCatalogMetadata.validate(
                table(true, constraint("nn").notNull("id").build()), VgiCatalogMetadata.OFFER, null));
        var check = constraint("same-id").check("id >= 0").build();
        assertThrows(IllegalArgumentException.class, () -> VgiCatalogMetadata.validate(
                table(true, check, check), VgiCatalogMetadata.OFFER, null));
    }

    @Test void rejectsCheckExpressionsOutsideTheNegotiatedSubset() {
        assertThrows(IllegalArgumentException.class, () -> VgiCatalogMetadata.validate(
                table(true, constraint("check").check("abs(id) >= 0").build()), VgiCatalogMetadata.OFFER, null));
    }
}
