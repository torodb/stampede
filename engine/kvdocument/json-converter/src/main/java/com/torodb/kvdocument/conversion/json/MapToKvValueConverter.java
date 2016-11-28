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

package com.torodb.kvdocument.conversion.json;

import com.torodb.common.util.HexUtils;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInstant;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.ByteArrayKvMongoObjectId;
import com.torodb.kvdocument.values.heap.InstantKvInstant;
import com.torodb.kvdocument.values.heap.ListKvArray;
import com.torodb.kvdocument.values.heap.LongKvInstant;
import com.torodb.kvdocument.values.heap.MapKvDocument;
import com.torodb.kvdocument.values.heap.StringKvString;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MapToKvValueConverter {

  public KvDocument convert(Map<String, Object> source) {
    return (KvDocument) convertMap(source);
  }

  public List<KvDocument> convert(List<Map<String, Object>> source) {
    KvArray array = (KvArray) convertList(source);
    return StreamSupport.stream(array.spliterator(), false)
        .map(e -> (KvDocument) e).collect(Collectors.toList());
  }

  private KvValue<?> convertMap(Map<String, Object> source) {
    if (isSpecialObject(source)) {
      return buildSpecialObject(source);
    }
    LinkedHashMap<String, KvValue<?>> docMap = new LinkedHashMap<>();
    source.forEach((key, value) -> {
      String interned = key.intern();
      docMap.put(interned, convertValue(value));
    });
    return new MapKvDocument(docMap);
  }

  @SuppressWarnings("unchecked")
  private KvValue<?> convertValue(Object value) {
    if (value == null) {
      return KvNull.getInstance();
    }
    if (value instanceof Map) {
      return convertMap((Map<String, Object>) value);
    } else if (value instanceof List) {
      return convertList((List<Object>) value);
    } else if (value instanceof String) {
      return new StringKvString((String) value);
    } else if (value instanceof Integer) {
      return KvInteger.of((Integer) value);
    } else if (value instanceof Double) {
      return KvDouble.of((Double) value);
    } else if (value instanceof Long) {
      return KvLong.of((Long) value);
    } else if (value instanceof Boolean) {
      return KvBoolean.from((boolean) value);
    } else {
      throw new RuntimeException("Unexpected type value");
    }
  }

  private boolean isSpecialObject(Map<String, Object> map) {
    if (map != null && map.entrySet().size() == 1) {
      Entry<String, Object> next = map.entrySet().iterator().next();
      String key = next.getKey();
      Object value = next.getValue();
      if (key.startsWith("$") && value != null) {
        return true;
      }
    }
    return false;
  }

  private KvValue<?> buildSpecialObject(Map<String, Object> map) {
    Entry<String, Object> first = map.entrySet().iterator().next();
    String key = first.getKey();
    Object value = first.getValue();
    if ("$oid".equals(key) && value instanceof String) {
      return new ByteArrayKvMongoObjectId(HexUtils.hex2Bytes((String) value));
    }
    if ("$date".equals(key)) {
      return parseDate(key, value);
    }
    throw new RuntimeException("Unexpected special object type: " + key);
  }

  private KvInstant parseDate(String key, Object value) {
    if ("$date".equals(key) && value instanceof String) {
      try {
        DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        TemporalAccessor date = sdf.parse((String) value);
        return new InstantKvInstant(Instant.from(date));
      } catch (DateTimeParseException e) {
        throw new RuntimeException("Unexpected error parsing date", e);
      }
    }
    if ("$date".equals(key) && value instanceof Long) {
      return new LongKvInstant((Long) value);
    }
    if ("$date".equals(key) && value instanceof Double) {
      return new LongKvInstant(((Double) value).longValue());
    }
    throw new RuntimeException("Unexpected date object type: " + key);
  }

  private KvValue<?> convertList(List<?> values) {
    List<KvValue<?>> kvvalues = values.stream()
        .map(this::convertValue)
        .collect(Collectors.toList());
    return new ListKvArray(kvvalues);
  }
}
