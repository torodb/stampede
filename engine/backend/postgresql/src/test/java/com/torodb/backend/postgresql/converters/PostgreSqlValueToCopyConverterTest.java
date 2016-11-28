/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.postgresql.converters;

import static org.junit.Assert.assertEquals;

import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvMongoObjectId;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.heap.ByteArrayKvMongoObjectId;
import com.torodb.kvdocument.values.heap.InstantKvInstant;
import com.torodb.kvdocument.values.heap.LocalDateKvDate;
import com.torodb.kvdocument.values.heap.LocalTimeKvTime;
import com.torodb.kvdocument.values.heap.StringKvString;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;

/**
 *
 * @author gortiz
 */
public class PostgreSqlValueToCopyConverterTest {

  private static final PostgreSqlValueToCopyConverter visitor =
      PostgreSqlValueToCopyConverter.INSTANCE;
  private StringBuilder sb = new StringBuilder();

  public PostgreSqlValueToCopyConverterTest() {
    sb = new StringBuilder();
  }

  @Before
  public void clean() {
    sb.delete(0, sb.length());
  }

  @Test
  public void testBooleanTrue() {
    KvBoolean.TRUE.accept(visitor, sb);
    assertEquals("true", sb.toString());
  }

  @Test
  public void testBooleanFalse() {
    KvBoolean.FALSE.accept(visitor, sb);
    assertEquals("false", sb.toString());
  }

  @Test
  public void testNull() {
    KvNull.getInstance().accept(visitor, sb);
    assertEquals("true", sb.toString());
  }

  @Test
  public void testIntegerZero() {
    KvInteger.of(0).accept(visitor, sb);
    assertEquals("0", sb.toString());
  }

  @Test
  public void testIntegerPositive() {
    KvInteger.of(123).accept(visitor, sb);
    assertEquals("123", sb.toString());
  }

  @Test
  public void testIntegerNegative() {
    KvInteger.of(-3142).accept(visitor, sb);
    assertEquals("-3142", sb.toString());
  }

  @Test
  public void testLongZero() {
    KvLong.of(0).accept(visitor, sb);
    assertEquals("0", sb.toString());
  }

  @Test
  public void testLongPositive() {
    KvLong.of(123).accept(visitor, sb);
    assertEquals("123", sb.toString());
  }

  @Test
  public void testLongNegative() {
    KvLong.of(-3142).accept(visitor, sb);
    assertEquals("-3142", sb.toString());
  }

  @Test
  public void testDoubleZero() {
    KvDouble.of(0).accept(visitor, sb);
    assertEquals("0.0", sb.toString());
  }

  @Test
  public void testDoublePositive() {
    KvDouble.of(4.5).accept(visitor, sb);
    assertEquals("4.5", sb.toString());
  }

  @Test
  public void testDoubleNegative() {
    KvDouble.of(-4.5).accept(visitor, sb);
    assertEquals("-4.5", sb.toString());
  }

  @Test
  public void testStringSimple() {
    new StringKvString("simple string").accept(visitor, sb);
    assertEquals("simple string", sb.toString());
  }

  @Test
  public void testStringWithTab() {
    new StringKvString("a string with a \t").accept(visitor, sb);
    assertEquals("a string with a \\\t", sb.toString());
  }

  @Test
  public void testStringWithNewLine() {
    new StringKvString("a string with a \n").accept(visitor, sb);
    assertEquals("a string with a \\\n", sb.toString());
  }

  @Test
  public void testStringWithBackslash() {
    new StringKvString("a string with a \\").accept(visitor, sb);
    assertEquals("a string with a \\\\", sb.toString());
  }

  @Test
  public void testStringWithSpecials() {
    new StringKvString("a string with a \\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff")
        .accept(visitor, sb);
    assertEquals(
        "a string with a \\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, \\\\123, \\\\xa, \\\\xff",
        sb.toString());
  }

  @Test
  public void testStringNull() {
    new StringKvString("a string with a \\N and null literal").accept(visitor, sb);
    assertEquals("a string with a \\\\N and null literal", sb.toString());
  }

  @Test
  public void testMongoObjectId() {
    KvMongoObjectId mongoObjectIdValue = new ByteArrayKvMongoObjectId(
        new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}
    );
    mongoObjectIdValue.accept(visitor, sb);
    assertEquals("\\\\x0102030405060708090A0B0C", sb.toString());
  }

  @Test
  public void testDateTimeValue() {
    new InstantKvInstant(LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26).toInstant(
        ZoneOffset.UTC))
        .accept(visitor, sb);
    assertEquals("'2015-01-18T02:43:26Z'", sb.toString());
  }

  @Test
  public void testDateValue() {
    new LocalDateKvDate(LocalDate.of(2015, Month.JANUARY, 18))
        .accept(visitor, sb);
    assertEquals("'2015-01-18'", sb.toString());
  }

  @Test
  public void testTimeValue() {
    new LocalTimeKvTime(LocalTime.of(2, 43, 26))
        .accept(visitor, sb);
    assertEquals("'02:43:26'", sb.toString());
  }

}
