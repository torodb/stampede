package com.torodb.util.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.torodb.config.backend.Backend;

public class BackendSerializer extends JsonSerializer<Backend> {
	@Override
	public void serialize(Backend value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonProcessingException {
		jgen.writeStartObject();
		
		String fieldName = null;
		if (value.isPostgres()) {
			fieldName = "postgres";
		} else
		if (value.isGreenplum()) {
			fieldName = "greenplum";
		}
		
		if (fieldName != null) {
			jgen.writeObjectField(fieldName, value.getBackendImplementation());
		}
		
		jgen.writeEndObject();
	}
}