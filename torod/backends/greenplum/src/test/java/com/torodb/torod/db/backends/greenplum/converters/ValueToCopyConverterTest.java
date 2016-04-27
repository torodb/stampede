package com.torodb.torod.db.backends.greenplum.converters;

import com.google.common.collect.Lists;
import com.torodb.torod.core.subdocument.values.*;
import com.torodb.torod.core.subdocument.values.heap.*;
import com.torodb.torod.db.backends.greenplum.converters.GreenplumValueToCopyConverter;

import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.threeten.bp.*;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author gortiz
 */
public class ValueToCopyConverterTest {

    private static final GreenplumValueToCopyConverter visitor = GreenplumValueToCopyConverter.INSTANCE;
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
        ScalarBoolean.TRUE.accept(visitor, sb);
        assertEquals("true", sb.toString());
    }

    @Test
    public void testBooleanFalse() {
        ScalarBoolean.FALSE.accept(visitor, sb);
        assertEquals("false", sb.toString());
    }

    @Test
    public void testNull() {
        ScalarNull.getInstance().accept(visitor, sb);
        assertEquals("\\N", sb.toString());
    }

    @Test
    public void testIntegerZero() {
        ScalarInteger.of(0).accept(visitor, sb);
        assertEquals("0", sb.toString());
    }

    @Test
    public void testIntegerPositive() {
        ScalarInteger.of(123).accept(visitor, sb);
        assertEquals("123", sb.toString());
    }

    @Test
    public void testIntegerNegative() {
        ScalarInteger.of(-3142).accept(visitor, sb);
        assertEquals("-3142", sb.toString());
    }

    @Test
    public void testLongZero() {
        ScalarLong.of(0).accept(visitor, sb);
        assertEquals("0", sb.toString());
    }

    @Test
    public void testLongPositive() {
        ScalarLong.of(123).accept(visitor, sb);
        assertEquals("123", sb.toString());
    }

    @Test
    public void testLongNegative() {
        ScalarLong.of(-3142).accept(visitor, sb);
        assertEquals("-3142", sb.toString());
    }

    @Test
    public void testDoubleZero() {
        ScalarDouble.of(0).accept(visitor, sb);
        assertEquals("0.0", sb.toString());
    }

    @Test
    public void testDoublePositive() {
        ScalarDouble.of(4.5).accept(visitor, sb);
        assertEquals("4.5", sb.toString());
    }

    @Test
    public void testDoubleNegative() {
        ScalarDouble.of(-4.5).accept(visitor, sb);
        assertEquals("-4.5", sb.toString());
    }

    @Test
    public void testStringSimple() {
        new StringScalarString("simple string").accept(visitor, sb);
        assertEquals("simple string", sb.toString());
    }

    @Test
    public void testStringWithTab() {
        new StringScalarString("a string with a \t").accept(visitor, sb);
        assertEquals("a string with a \\\t", sb.toString());
    }

    @Test
    public void testStringWithNewLine() {
        new StringScalarString("a string with a \n").accept(visitor, sb);
        assertEquals("a string with a \\\n", sb.toString());
    }

    @Test
    public void testStringWithBackslash() {
        new StringScalarString("a string with a \\").accept(visitor, sb);
        assertEquals("a string with a \\\\", sb.toString());
    }

    @Test
    public void testStringWithSpecials() {
        new StringScalarString("a string with a \\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff").accept(visitor, sb);
        assertEquals("a string with a \\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, \\\\123, \\\\xa, \\\\xff", sb.toString());
    }

    @Test
    public void testStringNull() {
        new StringScalarString("a string with a \\N and null literal").accept(visitor, sb);
        assertEquals("a string with a \\\\N and null literal", sb.toString());
    }

    @Test
    public void testMongoObjectId() {
        ScalarMongoObjectId mongoObjectIdValue = new ByteArrayScalarMongoObjectId(
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}
        );
        mongoObjectIdValue.accept(visitor, sb);
        assertEquals("\\001\\002\\003\\004\\005\\006\\007\\010\\011\\012\\013\\014", sb.toString());
    }

    @Test
    public void testDateTimeValue() {
        new InstantScalarInstant(LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26).toInstant(ZoneOffset.UTC))
                .accept(visitor, sb);
        assertEquals("'2015-01-18T02:43:26Z'", sb.toString());
    }

    @Test
    public void testDateValue() {
        new LocalDateScalarDate(LocalDate.of(2015, Month.JANUARY, 18))
                .accept(visitor, sb);
        assertEquals("'2015-01-18'", sb.toString());
    }

    @Test
    public void testTimeValue() {
        new LocalTimeScalarTime(LocalTime.of(2, 43, 26))
                .accept(visitor, sb);
        assertEquals("'02:43:26'", sb.toString());
    }

    @Test
    public void testArrayValue_Empty() {
        new ListScalarArray(Collections.<ScalarValue<?>>emptyList()).accept(visitor, sb);
        assertEquals("[]", sb.toString());
    }

    @Test
    public void testArrayValue_Int() {
        List<ScalarValue<?>> values = Lists.<ScalarValue<?>>newArrayList(
                ScalarInteger.of(0),
                ScalarInteger.of(1),
                ScalarInteger.of(-1)
        );
        new ListScalarArray(values).accept(visitor, sb);
        assertEquals("[0,1,-1]", sb.toString());
    }

    @Test
    public void testArrayValue_Long() {
        List<ScalarValue<?>> values = Lists.<ScalarValue<?>>newArrayList(
                ScalarLong.of(0),
                ScalarLong.of(1),
                ScalarLong.of(-1)
        );
        new ListScalarArray(values).accept(visitor, sb);
        assertEquals("[0,1,-1]", sb.toString());
    }

    @Test
    public void testArrayValue_Double() {
        List<ScalarValue<?>> values = Lists.<ScalarValue<?>>newArrayList(
                ScalarDouble.of(0),
                ScalarDouble.of(2),
                ScalarDouble.of(-2)
        );
        new ListScalarArray(values).accept(visitor, sb);
        assertEquals("[0.0,2.0,-2.0]", sb.toString());
    }

    @Test
    public void testArrayValue_Null() {
        List<ScalarValue<?>> values = Lists.<ScalarValue<?>>newArrayList(
                ScalarNull.getInstance()
        );
        new ListScalarArray(values).accept(visitor, sb);
        assertEquals("[null]", sb.toString());
    }

    @Test
    public void testArrayValue_SimpleStrings() {
        List<ScalarValue<?>> values = Lists.<ScalarValue<?>>newArrayList(
                new StringScalarString(""),
                new StringScalarString("a simple string"),
                new StringScalarString("another simple string"),
                new StringScalarString("yet another simple string")
        );
        new ListScalarArray(values).accept(visitor, sb);
        assertEquals("[\"\",\"a simple string\",\"another simple string\",\"yet another simple string\"]", sb.toString());
    }

    @Test
    public void testArrayValue_SpecialStrings() {
        List<ScalarValue<?>> values = Lists.<ScalarValue<?>>newArrayList(
                new StringScalarString(""),
                new StringScalarString("null"),
                new StringScalarString("\n"),
                new StringScalarString("\t"),
                new StringScalarString("\\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff")
        );
        new ListScalarArray(values).accept(visitor, sb);
        assertEquals("[\"\",\"null\",\"\\\n\",\"\\\t\",\"\\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, \\\\123, \\\\xa, \\\\xff\"]", sb.toString());
    }

    @Test
    public void testArray_MongoObjectId() {
        ScalarMongoObjectId objectId = new ByteArrayScalarMongoObjectId(
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}
        );
        List<ScalarValue<?>> values = Lists.<ScalarValue<?>>newArrayList(objectId);
        new ListScalarArray(values).accept(visitor, sb);
        assertEquals("[\"\\\\x0102030405060708090A0B0C\"]", sb.toString());
    }

    @Test
    public void testArrayValue_Instant() {
        ScalarInstant instant = new InstantScalarInstant(
                LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26).toInstant(ZoneOffset.UTC));
        List<ScalarValue<?>> values = Lists.<ScalarValue<?>>newArrayList(instant);
        new ListScalarArray(values).accept(visitor, sb);
        assertEquals("[\"2015-01-18T02:43:26Z\"]", sb.toString());
    }

    @Test
    public void testArrayValue_Date() {
        new ListScalarArray(Lists.<ScalarValue<?>>newArrayList(
                new LocalDateScalarDate(LocalDate.of(2015, Month.JANUARY, 18)))
        ).accept(visitor, sb);
        assertEquals("[\"2015-01-18\"]", sb.toString());
    }

    @Test
    public void testArrayValue_Time() {
        new ListScalarArray(Lists.<ScalarValue<?>>newArrayList(
                new LocalTimeScalarTime(LocalTime.of(2, 43, 26)))
        ).accept(visitor, sb);
        assertEquals("[\"02:43:26\"]", sb.toString());
    }

    @Test
    @Ignore
    public void testArrayValue_Pattern() {
    }


}

