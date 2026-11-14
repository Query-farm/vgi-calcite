package farm.query.vgicalcite;

import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.validate.ValidateVectorBufferVisitor;
import org.apache.arrow.vector.validate.ValidateVectorDataVisitor;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Validate wire data before any row decoding, including columns used only by COUNT(*). */
final class VgiBatchValidation {
    private VgiBatchValidation() {}

    static void validate(VectorSchemaRoot root, DictionaryProvider dictionaries, List<Field> expected) {
        try {
            if (root.getFieldVectors().size() != expected.size()) {
                throw new IllegalArgumentException("column count differs from binding");
            }
            // Validate buffers before reading offsets, indices, text or nested values.
            if (root.getRowCount() < 0) throw new IllegalArgumentException("Negative batch row count");
            for (FieldVector vector : root.getFieldVectors()) {
                if (vector.getValueCount() != root.getRowCount()) {
                    throw new IllegalArgumentException("Batch and column row counts differ");
                }
                vector.accept(BUFFER_VALIDATOR, null);
            }
            Set<Long> checkedDictionaries = new HashSet<>();
            for (int i = 0; i < expected.size(); i++) {
                FieldVector vector = root.getVector(i);
                vector.accept(DATA_VALIDATOR, null);
                Field expectedField = expected.get(i);
                if (!expectedField.isNullable() && vector.getNullCount() > 0) {
                    throw nullViolation(expectedField);
                }
                Field logical = vector.getField();
                var encoding = logical.getDictionary();
                if (encoding != null) {
                    if (!(logical.getType() instanceof ArrowType.Int)) {
                        throw new IllegalArgumentException("dictionary indices must be integers");
                    }
                    var dictionary = dictionaries == null ? null : dictionaries.lookup(encoding.getId());
                    if (dictionary == null) throw new IllegalArgumentException("Missing Arrow dictionary " + encoding.getId());
                    FieldVector values = dictionary.getVector();
                    if (checkedDictionaries.add(encoding.getId())) {
                        values.accept(BUFFER_VALIDATOR, null);
                        values.accept(DATA_VALIDATOR, null);
                    }
                    for (int row = 0; row < vector.getValueCount(); row++) {
                        if (vector.isNull(row)) continue;
                        long index = ArrowTypes.dictionaryIndex(vector, row);
                        if (index < 0 || index >= values.getValueCount()) {
                            throw new IllegalArgumentException("Dictionary indices invalid at column " + i + ", row " + row);
                        }
                        if (!expectedField.isNullable() && values.isNull((int) index)) {
                            throw nullViolation(expectedField);
                        }
                    }
                    logical = new Field(logical.getName(), new FieldType(logical.isNullable(),
                            values.getField().getType(), null, logical.getMetadata()), values.getField().getChildren());
                }
                if (!sameType(expected.get(i), logical, true)) {
                    throw new IllegalArgumentException("column " + i + " schema differs from binding");
                }
            }
        } catch (RuntimeException failure) {
            // Do not include vector contents in errors: malformed offsets must
            // never be dereferenced by a diagnostic formatter either.
            throw new IllegalStateException("VGI worker sent a malformed Arrow batch (full validation): "
                    + failure.getMessage(), failure);
        }
    }

    private static IllegalArgumentException nullViolation(Field field) {
        return new IllegalArgumentException("VGI returned NULL for non-nullable column " + field.getName());
    }

    private static boolean sameType(Field expected, Field actual, boolean compareName) {
        if ((compareName && !expected.getName().equals(actual.getName())) || !expected.getType().equals(actual.getType())
                || !Objects.equals(expected.getMetadata().get("ARROW:extension:name"),
                        actual.getMetadata().get("ARROW:extension:name"))
                || expected.getChildren().size() != actual.getChildren().size()) return false;
        for (int i = 0; i < expected.getChildren().size(); i++) {
            // Arrow Java canonicalizes collection element names (for example
            // Python's "item" becomes "$data$"). Struct field names remain
            // significant because they are addressable from SQL.
            boolean named = expected.getType() instanceof ArrowType.Struct;
            if (!sameType(expected.getChildren().get(i), actual.getChildren().get(i), named)) return false;
        }
        return true;
    }

    private static void firstOffset(ValueVector vector, boolean wide) {
        if (vector.getValueCount() == 0) return;
        long offset = wide ? vector.getOffsetBuffer().getLong(0) : vector.getOffsetBuffer().getInt(0);
        if (offset < 0) throw new IllegalArgumentException("Negative first Arrow offset");
    }

    // The reader creates vectors from the wire schema; sameType checks that
    // schema against the binding. Arrow 19's additional type visitor rejects
    // valid MapVector instances by treating them as ListVector, so validate
    // their buffers directly instead.
    private static final ValidateVectorBufferVisitor BUFFER_VALIDATOR = new ValidateVectorBufferVisitor();

    private static final ValidateVectorDataVisitor DATA_VALIDATOR = new ValidateVectorDataVisitor() {
        @Override public Void visit(BaseVariableWidthVector vector, Void unused) {
            firstOffset(vector, false);
            return super.visit(vector, unused);
        }

        @Override public Void visit(BaseLargeVariableWidthVector vector, Void unused) {
            firstOffset(vector, true);
            return super.visit(vector, unused);
        }

        @Override public Void visit(ListVector vector, Void unused) {
            firstOffset(vector, false);
            return super.visit(vector, unused);
        }

        @Override public Void visit(LargeListVector vector, Void unused) {
            firstOffset(vector, true);
            return super.visit(vector, unused);
        }

        @Override public Void visit(FixedSizeListVector vector, Void unused) {
            // Arrow 19's data visitor incorrectly requests an offset buffer
            // here. Fixed-size lists have none; structural validation already
            // checked the child length. Validate the child values recursively.
            if ((long) vector.getValueCount() * vector.getListSize() > vector.getDataVector().getValueCount()) {
                throw new IllegalArgumentException("Fixed-size list exceeds child vector length");
            }
            vector.getDataVector().accept(this, null);
            return null;
        }
    };
}
