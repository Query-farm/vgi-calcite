package farm.query.vgicalcite;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VgiBatchValidationTest {
    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), List.of());
    }

    @Test void rejectsOversizedAndNegativeStringOffsetsBeforeReadingValues() {
        var schema = new Schema(List.of(field("text", new ArrowType.Utf8())));
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            var vector = (VarCharVector) root.getVector(0);
            vector.setSafe(0, new byte[] {65}); root.setRowCount(1);
            assertDoesNotThrow(() -> VgiBatchValidation.validate(root, null, schema.getFields()));
            vector.getOffsetBuffer().setInt(4, 1 << 20);
            assertThrows(IllegalStateException.class, () -> VgiBatchValidation.validate(root, null, schema.getFields()));
            vector.getOffsetBuffer().setInt(4, 1);
            vector.getOffsetBuffer().setInt(0, -1);
            assertThrows(IllegalStateException.class, () -> VgiBatchValidation.validate(root, null, schema.getFields()));
        }
    }

    @Test void rejectsInvalidUtf8InsteadOfReplacingItDuringStringDecoding() {
        var schema = new Schema(List.of(field("text", new ArrowType.Utf8())));
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            var vector = (VarCharVector) root.getVector(0);
            vector.setSafe(0, new byte[] {(byte) 0xff, (byte) 0xfe}); root.setRowCount(1);
            var error = assertThrows(IllegalStateException.class,
                    () -> VgiBatchValidation.validate(root, null, schema.getFields()));
            assertTrue(error.getMessage().contains("UTF-8"));
        }
    }

    @Test void rejectsListOffsetsOutsideTheChildVector() {
        var list = new Field("items", FieldType.nullable(new ArrowType.List()),
                List.of(field("item", new ArrowType.Int(64, true))));
        var schema = new Schema(List.of(list));
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            var vector = (ListVector) root.getVector(0);
            vector.startNewValue(0);
            ((BigIntVector) vector.getDataVector()).setSafe(0, 7);
            vector.endValue(0, 1); root.setRowCount(1);
            assertDoesNotThrow(() -> VgiBatchValidation.validate(root, null, schema.getFields()));
            vector.getOffsetBuffer().setInt(4, 1 << 20);
            assertThrows(IllegalStateException.class, () -> VgiBatchValidation.validate(root, null, schema.getFields()));
        }
    }

    @Test void validatesFixedSizeListsWithoutRequiringAnOffsetBuffer() {
        var list = new Field("items", FieldType.nullable(new ArrowType.FixedSizeList(2)),
                List.of(field("$data$", new ArrowType.Int(64, true))));
        var schema = new Schema(List.of(list));
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            var vector = (FixedSizeListVector) root.getVector(0);
            vector.setNotNull(0);
            var data = (BigIntVector) vector.getDataVector();
            data.setSafe(0, 3); data.setSafe(1, 7); root.setRowCount(1);
            assertDoesNotThrow(() -> VgiBatchValidation.validate(root, null, root.getSchema().getFields()));
            assertEquals(List.of(3L, 7L), ArrowTypes.read(vector, 0));
        }
    }

    @Test void validatesDictionaryValuesAndAllIndicesIncludingUnusedRows() {
        var encoding = new DictionaryEncoding(42, false, new ArrowType.Int(32, true));
        var indicesField = new Field("text", new FieldType(true, new ArrowType.Int(32, true), encoding), List.of());
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(new Schema(List.of(indicesField)), allocator);
             var values = new VarCharVector("values", allocator)) {
            root.allocateNew(); values.allocateNew();
            values.setSafe(0, new byte[] {65}); values.setValueCount(1);
            var provider = new DictionaryProvider.MapDictionaryProvider(new Dictionary(values, encoding));
            var indices = (IntVector) root.getVector(0);
            indices.setSafe(0, 0); indices.setSafe(1, 0); root.setRowCount(2);
            var expected = List.of(field("text", new ArrowType.Utf8()));
            assertDoesNotThrow(() -> VgiBatchValidation.validate(root, provider, expected));
            indices.set(1, 1 << 20);
            var error = assertThrows(IllegalStateException.class, () -> VgiBatchValidation.validate(root, provider, expected));
            assertTrue(error.getMessage().contains("Dictionary indices invalid"));
            indices.set(1, 0);
            values.getDataBuffer().setByte(0, 0xff);
            assertThrows(IllegalStateException.class, () -> VgiBatchValidation.validate(root, provider, expected));
        }
    }

    @Test void rejectsSchemaChangesAndRowCountDisagreement() {
        var schema = new Schema(List.of(field("n", new ArrowType.Int(64, true))));
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew(); ((BigIntVector) root.getVector(0)).setSafe(0, 7); root.setRowCount(1);
            assertThrows(IllegalStateException.class, () -> VgiBatchValidation.validate(root, null,
                    List.of(field("n", new ArrowType.Utf8()))));
            assertThrows(IllegalStateException.class, () -> VgiBatchValidation.validate(root, null,
                    List.of(field("other", new ArrowType.Int(64, true)))));
            root.getVector(0).setValueCount(0);
            assertThrows(IllegalStateException.class, () -> VgiBatchValidation.validate(root, null, schema.getFields()));
        }
    }
}
