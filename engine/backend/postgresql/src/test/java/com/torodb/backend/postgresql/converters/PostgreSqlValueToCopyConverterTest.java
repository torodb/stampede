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
import com.torodb.kvdocument.types.*;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.heap.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.math.BigDecimal;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author gortiz
 */
@RunWith(Parameterized.class)
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

  @Parameterized.Parameters(name = "{index} - {0}")
  public static Collection<Object[]> data() throws Exception {


    Collection<Object[]> allTests = Arrays.stream(new Object [][]{
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
            {"StringWithTab", new StringKvString("a string with a \t"), "a string with a \\\t"},
            {"StringWithNewLine", new StringKvString("a string with a \n"), "a string with a \\\n"},
            {"StringWithBackSlash", new StringKvString("a string with a \\"), "a string with a \\\\"},
            {
                    "StringWithSpecials",
                    new StringKvString("a string with a \\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff"),
                    "a string with a \\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, \\\\123, \\\\xa, \\\\xff"
            },
            {
                    "StringNull",
                    new StringKvString("a string with a \\N and null literal"),
                    "a string with a \\\\N and null literal"
            },
            {
                    "MongoObjectId",
                    new ByteArrayKvMongoObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}),
                    "\\\\x0102030405060708090A0B0C"
            },
            {
                    "DateTime",
                    new InstantKvInstant(LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26).toInstant(
                            ZoneOffset.UTC)),
                    "'2015-01-18T02:43:26Z'"
            },
            {"Date", new LocalDateKvDate(LocalDate.of(2015, Month.JANUARY, 18)), "'2015-01-18'"},
            {"Time", new LocalTimeKvTime(LocalTime.of(2, 43, 26)), "'02:43:26'"},
            {
                    "Binary",
                    new ByteSourceKvBinary(
                            KvBinary.KvBinarySubtype.MONGO_USER_DEFINED,
                            Byte.parseByte("1", 2),
                            ByteSource.wrap(new byte[]{0x12, 0x34, 0x56, 0x78, (byte) 0x9a})
                    ),
                    "\\\\x123456789A"
            },
            {"ZeroLong", KvLong.of(0), "0"},
            {"PositiveLong", KvLong.of(123456789L), "123456789"},
            {"NegativeLong", KvLong.of(-123456789L), "-123456789"},
            {"MinKey", KvMinKey.getInstance(), "false"},
            {"MaxKey", KvMaxKey.getInstance(), "true"},
            {"Undefined", KvUndefined.getInstance(), "true"},
            {"Deprecated", KvDeprecated.of("deprecate me"), "deprecate me"},
            {"Javascript", KvJavascript.of("alert('hello');"), "alert('hello');"},
            {
                    "JavascriptWithScope",
                    KvJavascriptWithScope.of("alert('hello');", new MapKvDocument(new LinkedHashMap<>(
                            ImmutableMap.<String, KvValue<?>>builder().
                                    put("a", KvInteger.of(123)).
                                    put("b", KvLong.of(0)).
                                    build()
                    ))),
                    "alert('hello');{a : 123, b : 0}"
            },
            {"ZeroDecimal128", KvDecimal128.of(0, 0), "0." + String.format("%6176s", "").replace(' ', '0')},
            //{"NaNDecimal128", KvDecimal128.of(0x7c00000000000000L, 0), "0." + String.format("%6176s", "").replace(' ', '0')},
            //{"InfiniteDecimal128", KvDecimal128.of(0x7800000000000000L, 0), "0." + String.format("%6176s", "").replace(' ', '0')},
            {"HighDecimal128", KvDecimal128.of(new BigDecimal("1000000000000000000000")), "1000000000000000000000"},
            {"NegativeDecimal128", KvDecimal128.of(new BigDecimal("-1000000000000000000")), "-1000000000000000000"},
            {"TinyDecimal128", KvDecimal128.of(new BigDecimal("0.0000000000000000001")), "0.0000000000000000001"},
            {"MongoRegex", KvMongoRegex.of("pattern", "esd"), "0.0000000000000000001"},
            {"Timestamp", new DefaultKvMongoTimestamp(27,3), "(27,3)"},
            {
                    "DbPointer",
                    KvMongoDbPointer.of(
                            "namespace",
                            new ByteArrayKvMongoObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc})
                    ),
                    "(namespace,\\\\x0102030405060708090A0B0C)"
            },
    }).collect(Collectors.toList());


    Stream<Class<?>> untestedKvTypes = KvTypeFinder.findAllKvTypes().filter(
            type -> allTests.stream().noneMatch(
                    toTest -> type.isAssignableFrom(((KvValue)toTest[1]).getType().getClass())
            )
    );

    String joinedTypes = untestedKvTypes.map(Class::getSimpleName).collect(Collectors.joining(", "));

    if(!joinedTypes.isEmpty())
    {
      throw new Exception(joinedTypes+ " types aren't tested");
    }

    return allTests;
  }

  @Parameterized.Parameter(0)
  public String label;

  @Parameterized.Parameter(1)
  public KvValue value;

  @Parameterized.Parameter(2)
  public String expectedResult;



  @Test
  public void test() {
    value.accept(visitor, sb);
    assertEquals(expectedResult, sb.toString());
  }


  private static class KvTypeFinder{

    public static Stream<Class<?>> findAllKvTypes(){
      return find("com.torodb.kvdocument.types").stream().filter(
              (clazz) -> KvType.class.isAssignableFrom(clazz) &&
                      !bannedClasses.contains(clazz)
      );
    }

    private static final List<Class<?>> bannedClasses = Arrays.asList(new Class<?>[]{
            NonExistentType.class,
            GenericType.class,
            ArrayType.class,
            DocumentType.class
    });

    private static final char PKG_SEPARATOR = '.';

    private static final char DIR_SEPARATOR = '/';

    private static final String CLASS_FILE_SUFFIX = ".class";

    private static final String BAD_PACKAGE_ERROR = "Unable to get resources from path '%s'. Are you sure the package '%s' exists?";

    private static List<Class<?>> find(String scannedPackage) {
      String scannedPath = scannedPackage.replace(PKG_SEPARATOR, DIR_SEPARATOR);
      URL scannedUrl = Thread.currentThread().getContextClassLoader().getResource(scannedPath);
      if (scannedUrl == null) {
        throw new IllegalArgumentException(String.format(BAD_PACKAGE_ERROR, scannedPath, scannedPackage));
      }
      File scannedDir = new File(scannedUrl.getFile());
      List<Class<?>> classes = new ArrayList<Class<?>>();
      for (File file : scannedDir.listFiles()) {
        classes.addAll(find(file, scannedPackage));
      }
      return classes;
    }

    private static List<Class<?>> find(File file, String scannedPackage) {
      List<Class<?>> classes = new ArrayList<Class<?>>();
      String resource = scannedPackage + PKG_SEPARATOR + file.getName();
      if (file.isDirectory()) {
        for (File child : file.listFiles()) {
          classes.addAll(find(child, resource));
        }
      } else if (resource.endsWith(CLASS_FILE_SUFFIX)) {
        int endIndex = resource.length() - CLASS_FILE_SUFFIX.length();
        String className = resource.substring(0, endIndex);
        try {
          classes.add(Class.forName(className));
        } catch (ClassNotFoundException ignore) {
        }
      }
      return classes;
    }


  }

}
