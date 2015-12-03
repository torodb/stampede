package com.torodb.util.jackson;

import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.torodb.config.backend.Backend;
import com.torodb.config.backend.BackendImplementation;
import com.torodb.config.backend.greenplum.Greenplum;
import com.torodb.config.backend.postgres.Postgres;

public class BackendDeserializer extends JsonDeserializer<Backend> {
	@Override
	public Backend deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		Backend backend = new Backend();
		JsonNode node = jp.getCodec().readTree(jp);
		
		JsonNode backendImplementationNode = null;
		Class<? extends BackendImplementation> backendImplementationClass = null;
		Iterator<String> fieldNamesIterator = node.fieldNames();
		while (fieldNamesIterator.hasNext()) {
			String fieldName = fieldNamesIterator.next();
			backendImplementationNode = node.get(fieldName);
			if ("postgres".equals(fieldName)) {
				backendImplementationClass = Postgres.class;
				break;
			} else if ("greenplum".equals(fieldName)) {
				backendImplementationClass = Greenplum.class;
				break;
			}
		}
		
		if (backendImplementationClass != null) {
			backend.setBackendImplementation(
					jp.getCodec().treeToValue(backendImplementationNode, backendImplementationClass));
		}
		
		return backend;
	}
}