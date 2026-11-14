package farm.query.vgicalcite;

import farm.query.vgi.CatalogProfiles;
import farm.query.vgi.internal.ConstraintContract;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.TableInfo;

import java.util.List;

/** Bounded, read-only interpretation of canonical catalog constraint metadata. */
final class VgiCatalogMetadata {
    static final CatalogCapabilities OFFER = new CatalogCapabilities(
            ConstraintContract.READ_PROFILES, List.of(), List.of(), List.of());

    private VgiCatalogMetadata() {}

    static CatalogCapabilities selected(byte[] encoded) {
        CatalogCapabilities selected = CatalogProfiles.decode(encoded);
        if (!OFFER.read().containsAll(selected.read()) || !selected.ddl().isEmpty()
                || !selected.write().isEmpty() || !selected.transaction().isEmpty()
                || selected.read().size() != selected.read().stream().distinct().count()) {
            throw new IllegalStateException("Worker selected catalog capabilities outside the client's read-only offer");
        }
        if (selected.read().contains(CatalogProfiles.SCALAR_EXPRESSION_READ)
                && !selected.read().contains(CatalogProfiles.CONSTRAINT_RECORDS_READ)) {
            throw new IllegalStateException("Scalar expression metadata requires the constraint read profile");
        }
        return selected;
    }

    static void validate(TableInfo table, CatalogCapabilities selected, ConstraintContract.TableLookup lookup) {
        if (table.constraints().isEmpty()) return;
        if (!selected.read().containsAll(OFFER.read())) {
            throw new IllegalStateException("Table " + table.name() + " contains constraints without negotiated read profiles");
        }
        // The pinned SDK validates identities, ownership, foreign keys and the
        // bounded numeric CHECK grammar and evaluation context. It also derives
        // the required Arrow nullability. Reject inconsistent wire metadata;
        // never silently rewrite the worker's schema on the client.
        TableInfo validated = ConstraintContract.serve(table, lookup);
        if (!SchemaUtil.deserializeSchema(table.columns()).equals(SchemaUtil.deserializeSchema(validated.columns()))) {
            throw new IllegalStateException("Table " + table.name() + " has constraint/Arrow nullability disagreement");
        }
        // The adapter retains records in TableInfo, but does not publish keys to
        // the optimizer or claim write enforcement from read authorization.
    }
}
