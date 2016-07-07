package com.torodb.kvdocument.conversion.json;

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

import com.torodb.common.util.HexUtils;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInstant;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ByteArrayKVMongoObjectId;
import com.torodb.kvdocument.values.heap.InstantKVInstant;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.LongKVInstant;
import com.torodb.kvdocument.values.heap.MapKVDocument;
import com.torodb.kvdocument.values.heap.StringKVString;

public class MapToKVValueConverter {

    public KVDocument convert(Map<String, Object> source) {
        return (KVDocument) convertMap(source);
    }

    public List<KVDocument> convert(List<Map<String, Object>> source) {
        KVArray array = (KVArray) convertList(source);
        return StreamSupport.stream(array.spliterator(), false)
                .map(e -> (KVDocument) e).collect(Collectors.toList());
    }

    private KVValue<?> convertMap(Map<String, Object> source) {
        if (isSpecialObject(source)) {
            return buildSpecialObject(source);
        }
        LinkedHashMap<String, KVValue<?>> docHM = new LinkedHashMap<>();
        source.forEach((key,value)->{
        	String interned = key.intern();
        	docHM.put(interned, convertValue(value));
        });
        return new MapKVDocument(docHM);
    }

	@SuppressWarnings("unchecked")
    private KVValue<?> convertValue(Object value) {
		if (value == null) {
			return KVNull.getInstance();
		}
		if (value instanceof Map) {
			return convertMap((Map<String, Object>) value);
		} else if (value instanceof List) {
			return convertList((List<Object>) value);
		} else if (value instanceof String) {
			return new StringKVString((String) value);
		} else if (value instanceof Integer) {
			return KVInteger.of((Integer) value);
		} else if (value instanceof Double) {
			return KVDouble.of((Double) value);
		} else if (value instanceof Long) {
			return KVLong.of((Long) value);
		} else if (value instanceof Boolean) {
			return KVBoolean.from((boolean) value);
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

	private KVValue<?> buildSpecialObject(Map<String, Object> map) {
		Entry<String, Object> first = map.entrySet().iterator().next();
		String key = first.getKey();
		Object value = first.getValue();
		if ("$oid".equals(key) && value instanceof String) {
			return new ByteArrayKVMongoObjectId(HexUtils.hex2Bytes((String) value));
		}
		if ("$date".equals(key)) {
			return parseDate(key, value);
		}
		throw new RuntimeException("Unexpected special object type: " + key);
	}

	private KVInstant parseDate(String key, Object value) {
		if ("$date".equals(key) && value instanceof String) {
			try {
				DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
				TemporalAccessor date = sdf.parse((String) value);
				return new InstantKVInstant(Instant.from(date));
			} catch (DateTimeParseException e) {
				e.printStackTrace();
				throw new RuntimeException("Unexpected error parsing date");
			}
		}
		if ("$date".equals(key) && value instanceof Long) {
			return new LongKVInstant((Long) value);
		}
		if ("$date".equals(key) && value instanceof Double) {
			return new LongKVInstant(((Double) value).longValue());
		}
		throw new RuntimeException("Unexpected date object type: " + key);
	}

	private KVValue<?> convertList(List<?> values) {
		List<KVValue<?>> kvvalues = values.stream().map(this::convertValue).collect(Collectors.toList());
		return new ListKVArray(kvvalues);
	}
}
