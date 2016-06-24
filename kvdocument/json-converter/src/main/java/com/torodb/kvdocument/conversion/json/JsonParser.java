package com.torodb.kvdocument.conversion.json;

import java.io.InputStream;
import java.util.List;

import com.torodb.kvdocument.values.KVDocument;

public interface JsonParser {

    KVDocument createFromJson(String json);

    List<KVDocument> createListFromJson(String json);

	KVDocument createFrom(InputStream is);
	
	List<KVDocument> createListFrom(InputStream is);

    KVDocument createFromResource(String name);

    List<KVDocument> createListFromResource(String name);

}