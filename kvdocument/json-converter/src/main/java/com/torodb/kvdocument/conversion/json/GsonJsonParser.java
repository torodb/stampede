package com.torodb.kvdocument.conversion.json;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.torodb.kvdocument.values.KVDocument;

@SuppressWarnings("unchecked")
public class GsonJsonParser implements JsonParser {

	private static Gson gson = new Gson();

	private static MapToKVValueConverter converter = new MapToKVValueConverter();

	@Override
	public KVDocument createFromJson(String json) {
		Map<String, Object> map = gson.fromJson(json, new HashMap<String, Object>().getClass());
		return converter.convert(map);
	}

	@Override
	public KVDocument createFrom(InputStream is) {
		Map<String, Object> map = gson.fromJson(new InputStreamReader(is), new HashMap<String, Object>().getClass());
		return converter.convert(map);
	}

	@Override
	public KVDocument createFromResource(String name) {
		return createFrom(this.getClass().getClassLoader().getResourceAsStream(name));
	}

}
