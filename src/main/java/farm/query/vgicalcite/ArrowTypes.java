package farm.query.vgicalcite;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Arrow schema and value conversion to Calcite's enumerable internal carriers. */
public final class ArrowTypes {
    private ArrowTypes() {}

    public static RelDataType rowType(RelDataTypeFactory factory, Schema schema) {
        var builder = factory.builder();
        for (Field field : schema.getFields()) builder.add(field.getName(), type(factory, field));
        return builder.build();
    }

    public static RelDataType type(RelDataTypeFactory factory, Field field) {
        if (field.getMetadata().containsKey("ARROW:extension:name")) {
            throw new UnsupportedOperationException("VGI logical extension type needs an explicit mapping: "
                    + field.getMetadata().get("ARROW:extension:name") + " on " + field.getName());
        }
        ArrowType arrow = field.getType();
        if (field.getChildren().stream().anyMatch(ArrowTypes::containsDictionary)) {
            throw new UnsupportedOperationException("Nested Arrow dictionary encoding is not supported: " + field.getName());
        }
        RelDataType type = switch (arrow.getTypeID()) {
            case Bool -> factory.createSqlType(SqlTypeName.BOOLEAN);
            case Int -> {
                var integer = (ArrowType.Int) arrow;
                int bits = integer.getBitWidth() + (integer.getIsSigned() ? 0 : 1);
                if (bits > 64 && factory.getTypeSystem().getMaxNumericPrecision() < 20) {
                    throw new UnsupportedOperationException("uint64 requires DECIMAL(20); set Calcite typeSystem=" + VgiTypeSystem.class.getName());
                }
                yield bits <= 8 ? factory.createSqlType(SqlTypeName.TINYINT)
                        : bits <= 16 ? factory.createSqlType(SqlTypeName.SMALLINT)
                        : bits <= 32 ? factory.createSqlType(SqlTypeName.INTEGER)
                        : bits <= 64 ? factory.createSqlType(SqlTypeName.BIGINT)
                        : factory.createSqlType(SqlTypeName.DECIMAL, 20, 0);
            }
            case FloatingPoint -> factory.createSqlType(
                    ((ArrowType.FloatingPoint) arrow).getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE
                            ? SqlTypeName.DOUBLE : SqlTypeName.REAL);
            case Utf8, LargeUtf8 -> factory.createSqlType(SqlTypeName.VARCHAR);
            case Binary, LargeBinary, FixedSizeBinary -> factory.createSqlType(SqlTypeName.VARBINARY);
            case Decimal -> {
                var decimal = (ArrowType.Decimal) arrow;
                if (decimal.getPrecision() > factory.getTypeSystem().getMaxNumericPrecision()
                        || decimal.getScale() < 0 || decimal.getScale() > decimal.getPrecision()) {
                    throw unsupported(field);
                }
                yield factory.createSqlType(SqlTypeName.DECIMAL, decimal.getPrecision(), decimal.getScale());
            }
            case Date -> factory.createSqlType(SqlTypeName.DATE);
            case Time -> factory.createSqlType(SqlTypeName.TIME, 3);
            case Timestamp -> factory.createSqlType(((ArrowType.Timestamp) arrow).getTimezone() == null
                    ? SqlTypeName.TIMESTAMP : SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
            case List, LargeList, FixedSizeList -> factory.createArrayType(type(factory, field.getChildren().getFirst()), -1);
            case Struct -> rowType(factory, new Schema(field.getChildren()));
            case Map -> {
                List<Field> entries = field.getChildren().getFirst().getChildren();
                yield factory.createMapType(type(factory, entries.get(0)), type(factory, entries.get(1)));
            }
            case Null -> factory.createSqlType(SqlTypeName.NULL);
            default -> throw unsupported(field);
        };
        return factory.createTypeWithNullability(type, field.isNullable());
    }

    static Object read(FieldVector vector, int row) {
        return read(vector, row, null);
    }

    static Object read(FieldVector vector, int row, org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries) {
        if (vector.isNull(row)) return null;
        if (vector.getField().getDictionary() != null) {
            var encoding = vector.getField().getDictionary();
            var dictionary = dictionaries == null ? null : dictionaries.lookup(encoding.getId());
            if (dictionary == null) throw new IllegalStateException("Missing Arrow dictionary " + encoding.getId());
            int index = Math.toIntExact(dictionaryIndex(vector, row));
            if (index < 0 || index >= dictionary.getVector().getValueCount()) throw new IllegalStateException("Invalid Arrow dictionary index " + index);
            return read(dictionary.getVector(), index, dictionaries);
        }
        Object value = vector instanceof TimeStampVector timestamp ? timestamp.get(row) : vector.getObject(row);
        return value(vector.getField(), value);
    }

    static long dictionaryIndex(FieldVector vector, int row) {
        Object value = vector.getObject(row);
        return value instanceof Character ch ? ch : ((Number) value).longValue();
    }

    private static Object value(Field field, Object value) {
        if (value == null) return null;
        ArrowType arrow = field.getType();
        return switch (arrow.getTypeID()) {
            case Utf8, LargeUtf8 -> value.toString();
            case Binary, LargeBinary, FixedSizeBinary -> new ByteString((byte[]) value);
            case Int -> {
                var integer = (ArrowType.Int) arrow;
                Number number = value instanceof Character ch ? Integer.valueOf(ch) : (Number) value;
                int bits = integer.getBitWidth() + (integer.getIsSigned() ? 0 : 1);
                if (bits > 64) {
                    yield number instanceof BigInteger big ? new BigDecimal(big)
                            : new BigDecimal(Long.toUnsignedString(number.longValue()));
                }
                if (!integer.getIsSigned()) number = number.longValue() & ((1L << integer.getBitWidth()) - 1);
                if (bits <= 8) yield number.byteValue();
                if (bits <= 16) yield number.shortValue();
                if (bits <= 32) yield number.intValue();
                yield number.longValue();
            }
            case FloatingPoint -> {
                Number n = (Number) value;
                if (((ArrowType.FloatingPoint) arrow).getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE) yield n.doubleValue();
                yield n.floatValue();
            }
            case Date -> {
                if (value instanceof LocalDate date) yield Math.toIntExact(date.toEpochDay());
                if (value instanceof LocalDateTime date) yield Math.toIntExact(date.toLocalDate().toEpochDay());
                long n = ((Number) value).longValue();
                yield Math.toIntExact(((ArrowType.Date) arrow).getUnit() == org.apache.arrow.vector.types.DateUnit.DAY
                        ? n : Math.floorDiv(n, 86_400_000));
            }
            case Time -> {
                if (value instanceof LocalTime time) yield Math.toIntExact(time.toNanoOfDay() / 1_000_000);
                yield Math.toIntExact(millis(((Number) value).longValue(), ((ArrowType.Time) arrow).getUnit()));
            }
            case Timestamp -> value instanceof LocalDateTime time
                    ? time.toInstant(ZoneOffset.UTC).toEpochMilli()
                    : millis(((Number) value).longValue(), ((ArrowType.Timestamp) arrow).getUnit());
            case List, LargeList, FixedSizeList -> {
                List<Object> list = new ArrayList<>();
                for (Object item : (List<?>) value) list.add(value(field.getChildren().getFirst(), item));
                yield list;
            }
            case Struct -> {
                Map<?, ?> struct = (Map<?, ?>) value;
                Object[] fields = new Object[field.getChildren().size()];
                for (int i = 0; i < fields.length; i++) {
                    Field child = field.getChildren().get(i);
                    fields[i] = value(child, struct.get(child.getName()));
                }
                yield fields;
            }
            case Map -> {
                List<Field> entries = field.getChildren().getFirst().getChildren();
                Map<Object, Object> result = new LinkedHashMap<>();
                for (Object entry : (List<?>) value) {
                    Map<?, ?> pair = (Map<?, ?>) entry;
                    result.put(value(entries.get(0), pair.get(entries.get(0).getName())),
                            value(entries.get(1), pair.get(entries.get(1).getName())));
                }
                yield result;
            }
            default -> value;
        };
    }

    private static long millis(long value, org.apache.arrow.vector.types.TimeUnit unit) {
        return switch (unit) {
            case SECOND -> Math.multiplyExact(value, 1_000);
            case MILLISECOND -> value;
            case MICROSECOND -> Math.floorDiv(value, 1_000);
            case NANOSECOND -> Math.floorDiv(value, 1_000_000);
        };
    }

    static UnsupportedOperationException unsupported(Field field) {
        return new UnsupportedOperationException("Unsupported VGI Arrow field " + field.getName() + ": " + field.getType());
    }

    private static boolean containsDictionary(Field field) {
        return field.getDictionary() != null || field.getChildren().stream().anyMatch(ArrowTypes::containsDictionary);
    }
}
