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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.torodb.kvdocument.values.KvBinary;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDecimal128;
import com.torodb.kvdocument.values.KvDeprecated;
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvMaxKey;
import com.torodb.kvdocument.values.KvMinKey;
import com.torodb.kvdocument.values.KvMongoDbPointer;
import com.torodb.kvdocument.values.KvMongoJavascript;
import com.torodb.kvdocument.values.KvMongoJavascriptWithScope;
import com.torodb.kvdocument.values.KvMongoRegex;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvUndefined;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.ByteArrayKvMongoObjectId;
import com.torodb.kvdocument.values.heap.ByteSourceKvBinary;
import com.torodb.kvdocument.values.heap.DefaultKvMongoTimestamp;
import com.torodb.kvdocument.values.heap.InstantKvInstant;
import com.torodb.kvdocument.values.heap.LocalDateKvDate;
import com.torodb.kvdocument.values.heap.LocalTimeKvTime;
import com.torodb.kvdocument.values.heap.MapKvDocument;
import com.torodb.kvdocument.values.heap.StringKvString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/** @author gortiz */
@RunWith(Parameterized.class)
public class PostgreSqlValueToCopyConverterTest {

  private static final PostgreSqlValueToCopyConverter visitor =
      PostgreSqlValueToCopyConverter.INSTANCE;
  private StringBuilder sb = new StringBuilder();

  @Parameterized.Parameter(0)
  public String label;

  @Parameterized.Parameter(1)
  public KvValue value;

  @Parameterized.Parameter(2)
  public String expectedResult;

  public PostgreSqlValueToCopyConverterTest() {
    sb = new StringBuilder();
  }

  @Before
  public void clean() {
    sb.delete(0, sb.length());
  }

  @Parameterized.Parameters(name = "{index} - {0}")
  public static Collection<Object[]> data() throws Exception {

    Collection<Object[]> allTests =
        Arrays.stream(
                new Object[][] {
                  {"TrueBoolean", KvBoolean.TRUE, "true"},
                  {"FalseBoolean", KvBoolean.FALSE, "false"},
                  {"Null", KvNull.getInstance(), "true"},
                  {"ZeroInteger", KvInteger.of(0), "0"},
                  {"PositiveInteger", KvInteger.of(123), "123"},
                  {"NegativeInteger", KvInteger.of(-3421), "-3421"},
                  {"ZeroDouble", KvDouble.of(0), "0.0"},
                  {"PositiveDouble", KvDouble.of(4.5), "4.5"},
                  {"NegativeDouble", KvDouble.of(-4.5), "-4.5"},
                  {"NormalString", new StringKvString("simple string"), "simple string"},
                  {
                    "StringWithTab",
                    new StringKvString("a string with a \t"),
                    "a string with a \\\t"
                  },
                  {
                    "StringWithNewLine",
                    new StringKvString("a string with a \n"),
                    "a string with a \\\n"
                  },
                  {
                    "StringWithBackSlash",
                    new StringKvString("a string with a \\"),
                    "a string with a \\\\"
                  },
                  {
                    "StringWithSpecials",
                    new StringKvString(
                        "a string with a \\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff"),
                    "a string with a \\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, \\\\123, \\\\xa, \\\\xff"
                  },
                  {
                    "StringNull",
                    new StringKvString("a string with a \\N and null literal"),
                    "a string with a \\\\N and null literal"
                  },
                  {
                    "MongoObjectId",
                    new ByteArrayKvMongoObjectId(
                        new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}),
                    "\\\\x0102030405060708090A0B0C"
                  },
                  {
                    "DateTime",
                    new InstantKvInstant(
                        LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26)
                            .toInstant(ZoneOffset.UTC)),
                    "'2015-01-18T02:43:26Z'"
                  },
                  {
                    "Date",
                    new LocalDateKvDate(LocalDate.of(2015, Month.JANUARY, 18)),
                    "'2015-01-18'"
                  },
                  {"Time", new LocalTimeKvTime(LocalTime.of(2, 43, 26)), "'02:43:26'"},
                  {
                    "Binary",
                    new ByteSourceKvBinary(
                        KvBinary.KvBinarySubtype.MONGO_USER_DEFINED,
                        Byte.parseByte("1", 2),
                        ByteSource.wrap(new byte[] {0x12, 0x34, 0x56, 0x78, (byte) 0x9a})),
                    "\\\\x123456789A"
                  },
                  {"ZeroLong", KvLong.of(0), "0"},
                  {"PositiveLong", KvLong.of(123456789L), "123456789"},
                  {"NegativeLong", KvLong.of(-123456789L), "-123456789"},
                  {"MinKey", KvMinKey.getInstance(), "false"},
                  {"MaxKey", KvMaxKey.getInstance(), "true"},
                  {"Undefined", KvUndefined.getInstance(), "true"},
                  {"Deprecated", KvDeprecated.of("deprecate me"), "deprecate me"},
                  {"Javascript", KvMongoJavascript.of("alert('hello');"), "alert('hello');"},
                  {
                    "JavascriptWithScope",
                    KvMongoJavascriptWithScope.of(
                        "alert('hello');",
                        new MapKvDocument(
                            new LinkedHashMap<>(
                                ImmutableMap.<String, KvValue<?>>builder()
                                    .put("a", KvInteger.of(123))
                                    .put("b", KvLong.of(0))
                                    .build()))),
                    "alert('hello');{a : 123, b : 0}"
                  },
                  //{"ZeroDecimal128", KvDecimal128.of(0, 0), "0." + String.format("%6176s", "").replace(' ', '0')},
                  //{"NaNDecimal128", KvDecimal128.of(0x7c00000000000000L, 0), "0." + String.format("%6176s", "").replace(' ', '0')},
                  //{"InfiniteDecimal128", KvDecimal128.of(0x7800000000000000L, 0), "0." + String.format("%6176s", "").replace(' ', '0')},
                  {
                    "HighDecimal128",
                    KvDecimal128.of(new BigDecimal("1000000000000000000000")),
                    "1000000000000000000000"
                  },
                  {
                    "NegativeDecimal128",
                    KvDecimal128.of(new BigDecimal("-1000000000000000000")),
                    "-1000000000000000000"
                  },
                  //{"TinyDecimal128", KvDecimal128.of(new BigDecimal("0.0000000000000000001")), "0.0000000000000000001"},
                  {"MongoRegex", KvMongoRegex.of("pattern", "esd"), "pattern,esd"},
                  {"Timestamp", new DefaultKvMongoTimestamp(27, 3), "(27,3)"},
                  {
                    "DbPointer",
                    KvMongoDbPointer.of(
                        "namespace",
                        new ByteArrayKvMongoObjectId(
                            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc})),
                    "(namespace,\\\\x0102030405060708090A0B0C)"
                  },
                })
            .collect(Collectors.toList());

    return allTests;
  }

  @Test
  public void test() {
    value.accept(visitor, sb);
    assertEquals(expectedResult, sb.toString());
  }
}
