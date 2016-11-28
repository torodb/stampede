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

package com.torodb.backend.derby.converters.json;

import com.google.common.collect.Maps;
import com.torodb.backend.converters.ValueConverter;
import com.torodb.backend.converters.json.BinaryValueToJsonConverter;
import com.torodb.backend.converters.json.BooleanValueToJsonConverter;
import com.torodb.backend.converters.json.DateValueToJsonConverter;
import com.torodb.backend.converters.json.DoubleValueToJsonConverter;
import com.torodb.backend.converters.json.InstantValueToJsonConverter;
import com.torodb.backend.converters.json.IntegerValueToJsonConverter;
import com.torodb.backend.converters.json.LongValueToJsonConverter;
import com.torodb.backend.converters.json.MongoObjectIdValueToJsonConverter;
import com.torodb.backend.converters.json.MongoTimestampValueToJsonConverter;
import com.torodb.backend.converters.json.NullValueToJsonConverter;
import com.torodb.backend.converters.json.StringValueToJsonConverter;
import com.torodb.backend.converters.json.TimeValueToJsonConverter;
import com.torodb.backend.converters.json.ValueToJsonConverterProvider;
import com.torodb.backend.derby.converters.array.DerbyValueToArrayConverterProvider;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.BinaryType;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.types.DateType;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.InstantType;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.types.LongType;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.types.MongoTimestampType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.types.StringType;
import com.torodb.kvdocument.types.TimeType;

import java.util.Map;

public class DerbyValueToJsonConverterProvider implements ValueToJsonConverterProvider {

  private static final long serialVersionUID = 1L;

  private final Map<Class<? extends KvType>, ValueConverter<?, ?>> converters;

  private DerbyValueToJsonConverterProvider() {
    converters = Maps.newHashMap();
    converters.put(ArrayType.class, new ArrayValueToJsonConverter(DerbyValueToArrayConverterProvider
        .getInstance()));
    converters.put(BooleanType.class, new BooleanValueToJsonConverter());
    converters.put(DateType.class, new DateValueToJsonConverter());
    converters.put(InstantType.class, new InstantValueToJsonConverter());
    converters.put(DoubleType.class, new DoubleValueToJsonConverter());
    converters.put(IntegerType.class, new IntegerValueToJsonConverter());
    converters.put(LongType.class, new LongValueToJsonConverter());
    converters.put(NullType.class, new NullValueToJsonConverter());
    converters.put(StringType.class, new StringValueToJsonConverter());
    converters.put(TimeType.class, new TimeValueToJsonConverter());
    converters.put(MongoObjectIdType.class, new MongoObjectIdValueToJsonConverter());
    converters.put(MongoTimestampType.class, new MongoTimestampValueToJsonConverter());
    converters.put(BinaryType.class, new BinaryValueToJsonConverter());
  }

  public static DerbyValueToJsonConverterProvider getInstance() {
    return ValueToJsonConverterProviderHolder.INSTANCE;
  }

  @Override
  public ValueConverter<?, ?> getConverter(KvType valueType) {
    ValueConverter<?, ?> converter = converters.get(valueType.getClass());
    if (converter == null) {
      throw new AssertionError("There is no converter that converts "
          + "elements of type " + valueType);
    }
    return converter;
  }

  private static class ValueToJsonConverterProviderHolder {

    private static final DerbyValueToJsonConverterProvider INSTANCE =
        new DerbyValueToJsonConverterProvider();
  }

  //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return DerbyValueToJsonConverterProvider.getInstance();
  }
}
