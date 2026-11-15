package farm.query.vgicalcite;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class VgiDriverTest {
    private Connection open() throws Exception {
        Properties properties = new Properties();
        return DriverManager.getConnection("jdbc:vgi-calcite:", properties);
    }

    private void assertAggregate(String sql, String sum, Double average) throws Exception {
        try (var connection = open(); var statement = connection.createStatement();
             var result = statement.executeQuery(sql)) {
            var metadata = result.getMetaData();
            assertEquals(Types.DECIMAL, metadata.getColumnType(1));
            assertEquals(38, metadata.getPrecision(1));
            assertEquals(0, metadata.getScale(1));
            assertEquals(BigDecimal.class.getName(), metadata.getColumnClassName(1));
            assertEquals(Types.DOUBLE, metadata.getColumnType(2));
            assertTrue(result.next());
            assertEquals(sum == null ? null : new BigDecimal(sum), result.getObject(1));
            assertEquals(average, result.getObject(2));
            assertFalse(result.next());
        }
    }

    @Test void widensSignedBigintExtremesBeforeAccumulation() throws Exception {
        for (String value : new String[]{"9223372036854775807", "-9223372036854775808"}) {
            assertAggregate("select sum(n), avg(n) from (values (cast(" + value
                            + " as bigint)), (cast(" + value + " as bigint))) t(n)",
                    new BigDecimal(value).multiply(BigDecimal.valueOf(2)).toPlainString(),
                    Double.valueOf(value));
        }
    }

    @Test void widensEveryIntegerWidthAndPreservesNullSemantics() throws Exception {
        for (String type : new String[]{"tinyint", "smallint", "integer", "bigint"}) {
            assertAggregate("select sum(n), avg(n) from (values (cast(1 as " + type
                    + ")), (cast(2 as " + type + ")), (cast(null as " + type + "))) t(n)", "3", 1.5d);
        }
        assertAggregate("select sum(n), avg(n) from (values (cast(null as bigint))) t(n)", null, null);
        assertAggregate("select sum(n), avg(n) from (values (cast(1 as bigint))) t(n) where false", null, null);
    }

    @Test void distinctAndFilteredSumsUseDecimalAccumulators() throws Exception {
        assertAggregate("select sum(distinct n), avg(distinct n) from (values "
                        + "(cast(9223372036854775807 as bigint)), (cast(9223372036854775806 as bigint)), "
                        + "(cast(9223372036854775807 as bigint)), (cast(null as bigint))) t(n)",
                "18446744073709551613", 9.223372036854776E18);
        assertAggregate("select sum(n) filter (where keep), avg(n) filter (where keep) from (values "
                        + "(cast(9223372036854775807 as bigint), true), "
                        + "(cast(9223372036854775807 as bigint), true), (cast(1 as bigint), false)) t(n, keep)",
                "18446744073709551614", 9.223372036854776E18);
    }

    @Test void groupedAndNestedAggregatesRetainTheWiderType() throws Exception {
        assertAggregate("select sum(total), avg(total) from (select k, sum(n) as total from (values "
                        + "(1, cast(9223372036854775807 as bigint)), (1, cast(9223372036854775807 as bigint)), "
                        + "(2, cast(9223372036854775807 as bigint)), (2, cast(9223372036854775807 as bigint))) t(k,n) "
                        + "group by k having sum(n) > 9223372036854775807)",
                "36893488147419103228", 1.8446744073709552E19);
    }

    @Test void windowFramesUseWideAccumulators() throws Exception {
        try (var connection = open(); var statement = connection.createStatement();
             var result = statement.executeQuery("select sum(n) over w, avg(n) over w from (values "
                     + "(1, cast(9223372036854775807 as bigint)), (2, cast(9223372036854775807 as bigint)), "
                     + "(3, cast(null as bigint)), (4, cast(-9223372036854775808 as bigint))) t(id,n) "
                     + "window w as (order by id rows between 1 preceding and current row) order by id")) {
            String[] sums = {"9223372036854775807", "18446744073709551614", "9223372036854775807", "-9223372036854775808"};
            for (String sum : sums) {
                assertTrue(result.next());
                assertEquals(new BigDecimal(sum), result.getObject(1));
                assertEquals(sum.startsWith("-") ? -9.223372036854776E18 : 9.223372036854776E18,
                        result.getDouble(2));
            }
            assertFalse(result.next());
        }
    }

    @Test void preparedParametersArePromotedOnEachExecution() throws Exception {
        try (var connection = open(); var statement = connection.prepareStatement(
                "select sum(cast(? as bigint)), avg(cast(? as bigint)) from (values (1), (2)) t(n)")) {
            assertEquals(Types.DECIMAL, statement.getMetaData().getColumnType(1));
            assertEquals(Types.BIGINT, statement.getParameterMetaData().getParameterType(1));
            for (long value : new long[]{Long.MAX_VALUE, Long.MIN_VALUE, 1}) {
                statement.setLong(1, value);
                statement.setLong(2, value);
                try (var result = statement.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(BigDecimal.valueOf(value).multiply(BigDecimal.valueOf(2)), result.getObject(1));
                    assertEquals((double) value, result.getDouble(2));
                }
            }
        }
    }

    @Test void leavesDecimalScaleAndFloatingPointInputsIntact() throws Exception {
        try (var connection = open(); var statement = connection.createStatement();
             var result = statement.executeQuery("select sum(n), avg(n), sum(cast(n as double)), "
                     + "avg(cast(n as double)), count(n), min(n), max(n) from (values "
                     + "(cast(1.25 as decimal(20,2))), (cast(2.50 as decimal(20,2)))) t(n)")) {
            assertTrue(result.next());
            assertEquals(new BigDecimal("3.75"), result.getObject(1));
            assertEquals(1.875d, result.getObject(2));
            assertEquals(3.75d, result.getObject(3));
            assertEquals(1.875d, result.getObject(4));
            assertEquals(2L, result.getObject(5));
            assertEquals(new BigDecimal("1.25"), result.getObject(6));
            assertEquals(new BigDecimal("2.50"), result.getObject(7));
        }
    }

    @Test void viewsExposeTheSameWideResultAsDirectQueries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("model", "inline:{version:'1.0', defaultSchema:'test', schemas:[{name:'test', tables:["
                + "{name:'totals', type:'view', sql:'select sum(n) as total from (values "
                + "(cast(9223372036854775807 as bigint)), (cast(9223372036854775807 as bigint))) t(n)'},"
                + "{name:'nested', type:'view', sql:'select total as renamed from \"totals\"'}]}]}");
        try (var connection = DriverManager.getConnection("jdbc:vgi-calcite:", properties);
             var statement = connection.createStatement();
             var result = statement.executeQuery("select * from \"totals\"")) {
            assertTrue(result.next());
            assertEquals(new BigDecimal("18446744073709551614"), result.getObject(1));
            assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        }
        try (var connection = DriverManager.getConnection("jdbc:vgi-calcite:", properties);
             var statement = connection.prepareStatement("select x from \"nested\" as v(x)")) {
            assertEquals(Types.DECIMAL, statement.getMetaData().getColumnType(1));
            assertEquals("X", statement.getMetaData().getColumnLabel(1));
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(new BigDecimal("18446744073709551614"), result.getObject(1));
            }
        }
    }

    @Test void commonTableExpressionsAndUnionsPreserveWideTypes() throws Exception {
        assertAggregate("with totals as (select sum(n) as total from (values "
                        + "(cast(9223372036854775807 as bigint)), (cast(9223372036854775807 as bigint))) t(n)) "
                        + "select sum(total), avg(total) from (select total from totals "
                        + "union all select cast(0 as bigint) as total from (values (1)) t(x))",
                "18446744073709551614", 9.223372036854776E18);
    }

    @Test void leavesOtherCalciteConnectionsUnmodified() throws Exception {
        try (var vgi = open(); var plain = DriverManager.getConnection("jdbc:calcite:");
             var statement = plain.createStatement();
             var result = statement.executeQuery("select sum(n) from (values (1), (2)) t(n)")) {
            assertTrue(result.next());
            assertEquals(3, result.getObject(1));
            assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
        }
    }
}
