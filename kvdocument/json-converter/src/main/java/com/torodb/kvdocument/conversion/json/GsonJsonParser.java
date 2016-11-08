/*
 * MongoWP - KVDocument: Gson Converter
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.kvdocument.conversion.json;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
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

	private static final MapToKVValueConverter converter = new MapToKVValueConverter();
	private static final HashMap<String, Object> sampleClass = new HashMap<String, Object>();
	private static final Type type = new TypeToken<List<HashMap<String, Object>>>() {}.getType();

    @Override
    public KVDocument createFromJson(String json) {
        return converter.convert(gson.fromJson(json, sampleClass.getClass()));
    }

    @Override
    public List<KVDocument> createListFromJson(String json) {
        return converter.convert((List<Map<String, Object>>) gson.fromJson(json, type));
    }

	@Override
	public KVDocument createFrom(InputStream is) {
		return converter.convert(gson.fromJson(new InputStreamReader(is,Charsets.UTF_8), sampleClass.getClass()));
	}

    @Override
    public List<KVDocument> createListFrom(InputStream is) {
        return converter.convert((List<Map<String, Object>>) gson.fromJson(new InputStreamReader(is,Charsets.UTF_8), type));
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
