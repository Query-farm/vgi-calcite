package farm.query.vgicalcite;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ArrowTypesTest {
    @Test void unsignedBoundariesDoNotWrapOrNarrow() {
        try (var allocator = new RootAllocator(); var small = new UInt1Vector("u8", allocator);
             var medium = new UInt4Vector("u32", allocator); var large = new UInt8Vector("u64", allocator)) {
            small.allocateNew(1); small.set(0, 255); small.setValueCount(1);
            medium.allocateNew(1); medium.set(0, -1); medium.setValueCount(1);
            large.allocateNew(1); large.set(0, -1); large.setValueCount(1);
            assertEquals((short) 255, ArrowTypes.read(small, 0));
            assertEquals(4294967295L, ArrowTypes.read(medium, 0));
            assertEquals(new BigDecimal("18446744073709551615"), ArrowTypes.read(large, 0));
            assertEquals(SqlTypeName.SMALLINT, ArrowTypes.type(new JavaTypeFactoryImpl(), small.getField()).getSqlTypeName());
            assertThrows(UnsupportedOperationException.class, () -> ArrowTypes.type(new JavaTypeFactoryImpl(), large.getField()));
            assertEquals(20, ArrowTypes.type(new JavaTypeFactoryImpl(VgiTypeSystem.INSTANCE), large.getField()).getPrecision());
        }
    }

    @Test void temporalCarriersUseEpochDaysAndMillisecondsIncludingNegativeEpochs() {
        try (var allocator = new RootAllocator(); var date = new DateDayVector("d", allocator);
             var time = new TimeMicroVector("t", allocator); var timestamp = new TimeStampMicroVector("ts", allocator)) {
            date.allocateNew(1); date.set(0, -1); date.setValueCount(1);
            time.allocateNew(1); time.set(0, 1_234_567); time.setValueCount(1);
            timestamp.allocateNew(1); timestamp.set(0, -1); timestamp.setValueCount(1);
            assertEquals(-1, ArrowTypes.read(date, 0));
            assertEquals(1234, ArrowTypes.read(time, 0));
            assertEquals(-1L, ArrowTypes.read(timestamp, 0));
        }
    }

    @Test void floatsKeepCalciteCarrierWidths() {
        try (var allocator = new RootAllocator(); var single = new Float4Vector("f", allocator);
             var doublePrecision = new Float8Vector("d", allocator)) {
            single.allocateNew(1); single.set(0, 1.5f); single.setValueCount(1);
            doublePrecision.allocateNew(1); doublePrecision.set(0, 2.5); doublePrecision.setValueCount(1);
            assertInstanceOf(Float.class, ArrowTypes.read(single, 0));
            assertInstanceOf(Double.class, ArrowTypes.read(doublePrecision, 0));
        }
    }

    @Test void dictionariesYieldValuesRatherThanIndicesAndRetainNulls() {
        var encoding = new DictionaryEncoding(42, false, new ArrowType.Int(32, true));
        var field = new Field("encoded", new FieldType(true, new ArrowType.Int(32, true), encoding), null);
        try (var allocator = new RootAllocator(); var indices = (IntVector) field.createVector(allocator);
             var values = new VarCharVector("values", allocator)) {
            values.allocateNew(); values.setSafe(0, "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)); values.setValueCount(1);
            indices.allocateNew(2); indices.set(0, 0); indices.setNull(1); indices.setValueCount(2);
            // Provider does not own this test's vectors: close them explicitly above.
            var provider = new DictionaryProvider.MapDictionaryProvider(new Dictionary(values, encoding));
            assertEquals("hello", ArrowTypes.read(indices, 0, provider));
            assertNull(ArrowTypes.read(indices, 1, provider));
            assertThrows(IllegalStateException.class, () -> ArrowTypes.read(indices, 0));
        }
    }

    @Test void unsupportedTypesFailInsteadOfBecomingStrings() {
        var factory = new JavaTypeFactoryImpl();
        var duration = Field.nullable("elapsed", new ArrowType.Duration(TimeUnit.NANOSECOND));
        assertThrows(UnsupportedOperationException.class, () -> ArrowTypes.type(factory, duration));
        var huge = Field.nullable("decimal256", new ArrowType.Decimal(76, 0, 256));
        assertThrows(UnsupportedOperationException.class, () -> ArrowTypes.type(factory, huge));
        var opaque = new Field("opaque", new FieldType(true, new ArrowType.Binary(), null,
                Map.of("ARROW:extension:name", "arrow.opaque")), List.of());
        assertThrows(UnsupportedOperationException.class, () -> ArrowTypes.type(factory, opaque));
    }

    @Test void nestedValuesAreCopiedIntoCalciteCarriers() {
        Field list = new Field("items", FieldType.nullable(new ArrowType.List()),
                List.of(Field.nullable("item", new ArrowType.Utf8())));
        Field struct = new Field("person", FieldType.nullable(new ArrowType.Struct()),
                List.of(Field.nullable("age", new ArrowType.Int(32, true)), Field.nullable("name", new ArrowType.Utf8())));
        Field map = new Field("attributes", FieldType.nullable(new ArrowType.Map(false)), List.of(
                new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(
                        Field.notNullable("key", new ArrowType.Utf8()), Field.nullable("value", new ArrowType.Int(64, true))))));
        try (var allocator = new RootAllocator(); var listVector = list.createVector(allocator);
             var structVector = struct.createVector(allocator); var mapVector = map.createVector(allocator)) {
            listVector.allocateNew(); structVector.allocateNew(); mapVector.allocateNew();
            farm.query.vgi.internal.VectorScalarCodec.write(listVector, 0, java.util.Arrays.asList("first", null, "é"));
            farm.query.vgi.internal.VectorScalarCodec.write(structVector, 0, Map.of("age", 7L, "name", "Alice"));
            farm.query.vgi.internal.VectorScalarCodec.write(mapVector, 0, List.of(Map.of("key", "x", "value", 3L)));
            listVector.setValueCount(1); structVector.setValueCount(1); mapVector.setValueCount(1);
            Object copied = ArrowTypes.read(listVector, 0);
            assertEquals(java.util.Arrays.asList("first", null, "é"), copied);
            assertArrayEquals(new Object[] {7, "Alice"}, (Object[]) ArrowTypes.read(structVector, 0));
            assertEquals(Map.of("x", 3L), ArrowTypes.read(mapVector, 0));
            listVector.clear();
            assertEquals(java.util.Arrays.asList("first", null, "é"), copied);
        }
    }
}
