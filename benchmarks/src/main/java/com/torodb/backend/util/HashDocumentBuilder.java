package com.torodb.backend.util;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.LinkedHashMap;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSource;
import com.torodb.common.util.HexUtils;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ByteArrayKVMongoObjectId;
import com.torodb.kvdocument.values.heap.ByteSourceKVBinary;
import com.torodb.kvdocument.values.heap.DefaultKVMongoTimestamp;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.LocalDateKVDate;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;
import com.torodb.kvdocument.values.heap.LongKVInstant;
import com.torodb.kvdocument.values.heap.MapKVDocument;
import com.torodb.kvdocument.values.heap.StringKVString;

public class HashDocumentBuilder {

	private final LinkedHashMap<String, KVValue<?>> map;
	private boolean built;

	public HashDocumentBuilder() {
		this.map = new LinkedHashMap<>();
		built = false;
	}

	public HashDocumentBuilder append(String fieldName) {
		Preconditions.checkState(!built);
		map.put(fieldName, KVNull.getInstance());
		return this;
	}

	public HashDocumentBuilder append(String fieldName, Integer value) {
		Preconditions.checkState(!built);
		map.put(fieldName, KVInteger.of(value));
		return this;
	}

	public HashDocumentBuilder append(String fieldName, Double value) {
		Preconditions.checkState(!built);
		map.put(fieldName, KVDouble.of(value));
		return this;
	}

	public HashDocumentBuilder append(String fieldName, Long value) {
		Preconditions.checkState(!built);
		map.put(fieldName, KVLong.of(value));
		return this;
	}

	public HashDocumentBuilder append(String fieldName, String value) {
		Preconditions.checkState(!built);
		map.put(fieldName, new StringKVString(value));
		return this;
	}

	public HashDocumentBuilder append(String fieldName, Boolean value) {
		Preconditions.checkState(!built);
		map.put(fieldName, KVBoolean.from(value));
		return this;
	}

	public HashDocumentBuilder append(String fieldName, LocalTime value) {
		Preconditions.checkState(!built);
		map.put(fieldName, new LocalTimeKVTime(value));
		return this;
	}

	public HashDocumentBuilder append(String fieldName, LocalDate value) {
		Preconditions.checkState(!built);
		map.put(fieldName, new LocalDateKVDate(value));
		return this;
	}

	public HashDocumentBuilder appendInstant(String fieldName, long value) {
		Preconditions.checkState(!built);
		map.put(fieldName, new LongKVInstant(value));
		return this;
	}

	public HashDocumentBuilder appendMongoTS(String fieldName, int seconds, int ordinal) {
		Preconditions.checkState(!built);
		map.put(fieldName, new DefaultKVMongoTimestamp(seconds, ordinal));
		return this;
	}

	public HashDocumentBuilder appendMongoId(String fieldName, String hexId) {
		Preconditions.checkState(!built);
		map.put(fieldName, new ByteArrayKVMongoObjectId(HexUtils.hex2Bytes(hexId)));
		return this;
	}

	public HashDocumentBuilder appendMongoId(String fieldName, ByteSource byteSource) {
		Preconditions.checkState(!built);
		map.put(fieldName, new ByteSourceKVBinary(KVBinarySubtype.MONGO_GENERIC, (byte) 0, byteSource));
		return this;
	}

	public HashDocumentBuilder append(String fieldName, KVValue<?> value) {
		Preconditions.checkState(!built);
		map.put(fieldName, value);
		return this;
	}

	public HashDocumentBuilder appendArr(String fieldName, KVValue<?>... value) {
		Preconditions.checkState(!built);
		ListKVArray arr = new ListKVArray(Arrays.asList(value));
		map.put(fieldName, arr);
		return this;
	}

	public KVDocument build() {
		MapKVDocument doc = new MapKVDocument(map);
		built = true;
		return doc;
	}
}
