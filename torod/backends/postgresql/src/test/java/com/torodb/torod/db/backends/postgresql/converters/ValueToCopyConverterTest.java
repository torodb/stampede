package com.torodb.torod.db.backends.postgresql.converters;

import com.torodb.torod.core.subdocument.values.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.threeten.bp.LocalDate;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.LocalTime;
import org.threeten.bp.Month;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author gortiz
 */
public class ValueToCopyConverterTest {

    private static ValueToCopyConverter visitor = ValueToCopyConverter.INSTANCE;
    private StringBuilder sb = new StringBuilder();

    public ValueToCopyConverterTest() {
        sb = new StringBuilder();
    }

    @Before
    public void clean() {
        sb.delete(0, sb.length());
    }

    @Test
    public void testBooleanTrue() {
        BooleanValue.TRUE.accept(visitor, sb);
        assertEquals("true", sb.toString());
    }

    @Test
    public void testBooleanFalse() {
        BooleanValue.FALSE.accept(visitor, sb);
        assertEquals("false", sb.toString());
    }

    @Test
    public void testNull() {
        NullValue.INSTANCE.accept(visitor, sb);
        assertEquals("\\N", sb.toString());
    }

    @Test
    public void testIntegerZero() {
        new IntegerValue(0).accept(visitor, sb);
        assertEquals("0", sb.toString());
    }

    @Test
    public void testIntegerPositive() {
        new IntegerValue(123).accept(visitor, sb);
        assertEquals("123", sb.toString());
    }

    @Test
    public void testIntegerNegative() {
        new IntegerValue(-3142).accept(visitor, sb);
        assertEquals("-3142", sb.toString());
    }

    @Test
    public void testLongZero() {
        new LongValue(0).accept(visitor, sb);
        assertEquals("0", sb.toString());
    }

    @Test
    public void testLongPositive() {
        new LongValue(123).accept(visitor, sb);
        assertEquals("123", sb.toString());
    }

    @Test
    public void testLongNegative() {
        new LongValue(-3142).accept(visitor, sb);
        assertEquals("-3142", sb.toString());
    }

    @Test
    public void testDoubleZero() {
        new DoubleValue(0).accept(visitor, sb);
        assertEquals("0.0", sb.toString());
    }

    @Test
    public void testDoublePositive() {
        new DoubleValue(4.5).accept(visitor, sb);
        assertEquals("4.5", sb.toString());
    }

    @Test
    public void testDoubleNegative() {
        new DoubleValue(-4.5).accept(visitor, sb);
        assertEquals("-4.5", sb.toString());
    }

    @Test
    public void testStringSimple() {
        new StringValue("simple string").accept(visitor, sb);
        assertEquals("simple string", sb.toString());
    }

    @Test
    public void testStringWithTab() {
        new StringValue("a string with a \t").accept(visitor, sb);
        assertEquals("a string with a \\\t", sb.toString());
    }

    @Test
    public void testStringWithNewLine() {
        new StringValue("a string with a \n").accept(visitor, sb);
        assertEquals("a string with a \\\n", sb.toString());
    }

    @Test
    public void testStringWithBackslash() {
        new StringValue("a string with a \\").accept(visitor, sb);
        assertEquals("a string with a \\\\", sb.toString());
    }

    @Test
    public void testStringWithSpecials() {
        new StringValue("a string with a \\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff").accept(visitor, sb);
        assertEquals("a string with a \\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, \\\\123, \\\\xa, \\\\xff", sb.toString());
    }

    @Test
    public void testStringNull() {
        new StringValue("a string with a \\N and null literal").accept(visitor, sb);
        assertEquals("a string with a \\\\N and null literal", sb.toString());
    }

    @Test
    public void testTwelveBytes() {
        TwelveBytesValue twelveBytesValue = new TwelveBytesValue(
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}
        );
        twelveBytesValue.accept(visitor, sb);
        assertEquals("\\x0102030405060708090A0B0C", sb.toString());
    }

    @Test
    public void testDateTimeValue() {
        new DateTimeValue(LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26))
                .accept(visitor, sb);
        assertEquals("2015-01-18 02:43:26.0", sb.toString());
    }

    @Test
    public void testDateValue() {
        new DateValue(LocalDate.of(2015, Month.JANUARY, 18))
                .accept(visitor, sb);
        assertEquals("2015-01-18", sb.toString());
    }

    @Test
    public void testTimeValue() {
        new TimeValue(LocalTime.of(2, 43, 26))
                .accept(visitor, sb);
        assertEquals("02:43:26", sb.toString());
    }

    @Test
    @Ignore
    public void testPatternValue() {
    }

    @Test
    public void testArrayValue_Empty() {
        new ArrayValue.Builder().build().accept(visitor, sb);
        assertEquals("[]", sb.toString());
    }

    @Test
    public void testArrayValue_Int() {
        new ArrayValue.Builder()
                .add(new IntegerValue(0))
                .add(new IntegerValue(1))
                .add(new IntegerValue(-1))
                .build().accept(visitor, sb);
        assertEquals("[0,1,-1]", sb.toString());
    }

    @Test
    public void testArrayValue_Long() {
        new ArrayValue.Builder()
                .add(new LongValue(0))
                .add(new LongValue(1))
                .add(new LongValue(-1))
                .build().accept(visitor, sb);
        assertEquals("[0,1,-1]", sb.toString());
    }

    @Test
    public void testArrayValue_Double() {
        new ArrayValue.Builder()
                .add(new DoubleValue(0))
                .add(new DoubleValue(2))
                .add(new DoubleValue(-2))
                .build().accept(visitor, sb);
        assertEquals("[0.0,2.0,-2.0]", sb.toString());
    }

    @Test
    public void testArrayValue_Null() {
        new ArrayValue.Builder()
                .add(NullValue.INSTANCE)
                .build().accept(visitor, sb);
        assertEquals("[null]", sb.toString());
    }

    @Test
    public void testArrayValue_SimpleStrings() {
        new ArrayValue.Builder()
                .add(new StringValue(""))
                .add(new StringValue("a simple string"))
                .add(new StringValue("another simple string"))
                .add(new StringValue("yet another simple string"))
                .build().accept(visitor, sb);
        assertEquals("[\"\",\"a simple string\",\"another simple string\",\"yet another simple string\"]", sb.toString());
    }

    @Test
    public void testArrayValue_SpecialStrings() {
        new ArrayValue.Builder()
                .add(new StringValue(""))
                .add(new StringValue("null"))
                .add(new StringValue("\n"))
                .add(new StringValue("\t"))
                .add(new StringValue("\\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff"))
                .build().accept(visitor, sb);
        assertEquals("[\"\",\"null\",\"\\\n\",\"\\\t\",\"\\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, \\\\123, \\\\xa, \\\\xff\"]", sb.toString());
    }

    @Test
    public void testArray_TwelveBytes() {
        TwelveBytesValue twelveBytesValue = new TwelveBytesValue(
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}
        );
        new ArrayValue.Builder()
                .add(twelveBytesValue)
                .build().accept(visitor, sb);
        assertEquals("[\"\\\\x0102030405060708090A0B0C\"]", sb.toString());
    }

    @Test
    public void testArrayValue_DateTime() {
        new ArrayValue.Builder()
                .add(new DateTimeValue(LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26)))
                .build()
                .accept(visitor, sb);
        assertEquals("[\"2015-01-18 02:43:26.0\"]", sb.toString());
    }

    @Test
    public void testArrayValue_Date() {
        new ArrayValue.Builder()
                .add(new DateValue(LocalDate.of(2015, Month.JANUARY, 18)))
                .build()
                .accept(visitor, sb);
        assertEquals("[\"2015-01-18\"]", sb.toString());
    }

    @Test
    public void testArrayValue_Time() {
        new ArrayValue.Builder()
                .add(new TimeValue(LocalTime.of(2, 43, 26)))
                .build()
                .accept(visitor, sb);
        assertEquals("[\"02:43:26\"]", sb.toString());
    }

    @Test
    @Ignore
    public void testArrayValue_Pattern() {
    }


}
