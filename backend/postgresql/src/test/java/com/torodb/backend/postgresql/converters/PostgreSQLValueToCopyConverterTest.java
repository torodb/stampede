package com.torodb.backend.postgresql.converters;

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInstant;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVMongoObjectId;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ByteArrayKVMongoObjectId;
import com.torodb.kvdocument.values.heap.InstantKVInstant;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.LocalDateKVDate;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;
import com.torodb.kvdocument.values.heap.StringKVString;

/**
 *
 * @author gortiz
 */
public class PostgreSQLValueToCopyConverterTest {

    private static final PostgreSQLValueToCopyConverter visitor = PostgreSQLValueToCopyConverter.INSTANCE;
    private StringBuilder sb = new StringBuilder();

    public PostgreSQLValueToCopyConverterTest() {
        sb = new StringBuilder();
    }

    @Before
    public void clean() {
        sb.delete(0, sb.length());
    }

    @Test
    public void testBooleanTrue() {
        KVBoolean.TRUE.accept(visitor, sb);
        assertEquals("true", sb.toString());
    }

    @Test
    public void testBooleanFalse() {
        KVBoolean.FALSE.accept(visitor, sb);
        assertEquals("false", sb.toString());
    }

    @Test
    public void testNull() {
        KVNull.getInstance().accept(visitor, sb);
        assertEquals("true", sb.toString());
    }

    @Test
    public void testIntegerZero() {
        KVInteger.of(0).accept(visitor, sb);
        assertEquals("0", sb.toString());
    }

    @Test
    public void testIntegerPositive() {
        KVInteger.of(123).accept(visitor, sb);
        assertEquals("123", sb.toString());
    }

    @Test
    public void testIntegerNegative() {
        KVInteger.of(-3142).accept(visitor, sb);
        assertEquals("-3142", sb.toString());
    }

    @Test
    public void testLongZero() {
        KVLong.of(0).accept(visitor, sb);
        assertEquals("0", sb.toString());
    }

    @Test
    public void testLongPositive() {
        KVLong.of(123).accept(visitor, sb);
        assertEquals("123", sb.toString());
    }

    @Test
    public void testLongNegative() {
        KVLong.of(-3142).accept(visitor, sb);
        assertEquals("-3142", sb.toString());
    }

    @Test
    public void testDoubleZero() {
        KVDouble.of(0).accept(visitor, sb);
        assertEquals("0.0", sb.toString());
    }

    @Test
    public void testDoublePositive() {
        KVDouble.of(4.5).accept(visitor, sb);
        assertEquals("4.5", sb.toString());
    }

    @Test
    public void testDoubleNegative() {
        KVDouble.of(-4.5).accept(visitor, sb);
        assertEquals("-4.5", sb.toString());
    }

    @Test
    public void testStringSimple() {
        new StringKVString("simple string").accept(visitor, sb);
        assertEquals("simple string", sb.toString());
    }

    @Test
    public void testStringWithTab() {
        new StringKVString("a string with a \t").accept(visitor, sb);
        assertEquals("a string with a \\\t", sb.toString());
    }

    @Test
    public void testStringWithNewLine() {
        new StringKVString("a string with a \n").accept(visitor, sb);
        assertEquals("a string with a \\\n", sb.toString());
    }

    @Test
    public void testStringWithBackslash() {
        new StringKVString("a string with a \\").accept(visitor, sb);
        assertEquals("a string with a \\\\", sb.toString());
    }

    @Test
    public void testStringWithSpecials() {
        new StringKVString("a string with a \\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff").accept(visitor, sb);
        assertEquals("a string with a \\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, \\\\123, \\\\xa, \\\\xff", sb.toString());
    }

    @Test
    public void testStringNull() {
        new StringKVString("a string with a \\N and null literal").accept(visitor, sb);
        assertEquals("a string with a \\\\N and null literal", sb.toString());
    }

    @Test
    public void testMongoObjectId() {
        KVMongoObjectId mongoObjectIdValue = new ByteArrayKVMongoObjectId(
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}
        );
        mongoObjectIdValue.accept(visitor, sb);
        assertEquals("\\\\x0102030405060708090A0B0C", sb.toString());
    }

    @Test
    public void testDateTimeValue() {
        new InstantKVInstant(LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26).toInstant(ZoneOffset.UTC))
                .accept(visitor, sb);
        assertEquals("'2015-01-18T02:43:26Z'", sb.toString());
    }

    @Test
    public void testDateValue() {
        new LocalDateKVDate(LocalDate.of(2015, Month.JANUARY, 18))
                .accept(visitor, sb);
        assertEquals("'2015-01-18'", sb.toString());
    }

    @Test
    public void testTimeValue() {
        new LocalTimeKVTime(LocalTime.of(2, 43, 26))
                .accept(visitor, sb);
        assertEquals("'02:43:26'", sb.toString());
    }

}

