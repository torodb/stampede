package com.torodb.kvdocument.conversion.json;

import java.io.InputStream;

import com.torodb.kvdocument.values.KVDocument;

public interface JsonParser {

	KVDocument createFromJson(String json);

	KVDocument createFrom(InputStream is);

	KVDocument createFromResource(String name);

}