package com.torodb.kvdocument.conversion.json;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.torodb.kvdocument.values.KVDocument;

@SuppressWarnings("unchecked")
public class GsonJsonParser implements JsonParser {

	private static Gson gson = new Gson();

	private static MapToKVValueConverter converter = new MapToKVValueConverter();
	private static HashMap<String, Object> sampleClass = new HashMap<String, Object>();

    @Override
    public KVDocument createFromJson(String json) {
        return converter.convert(gson.fromJson(json, sampleClass.getClass()));
    }

    @Override
    public List<KVDocument> createListFromJson(String json) {
        return converter.convert((List<Map<String, Object>>) gson.fromJson(json, new TypeToken<List<HashMap<String, Object>>>() {}.getType()));
    }

	@Override
	public KVDocument createFrom(InputStream is) {
		return converter.convert(gson.fromJson(new InputStreamReader(is,Charsets.UTF_8), sampleClass.getClass()));
	}

    @Override
    public List<KVDocument> createListFrom(InputStream is) {
        return converter.convert((List<Map<String, Object>>) gson.fromJson(new InputStreamReader(is,Charsets.UTF_8), new TypeToken<List<HashMap<String, Object>>>() {}.getType()));
    }

	@Override
	public KVDocument createFromResource(String name) {
		return createFrom(this.getClass().getClassLoader().getResourceAsStream(name));
	}

    @Override
    public List<KVDocument> createListFromResource(String name) {
        return createListFrom(this.getClass().getClassLoader().getResourceAsStream(name));
    }

}
