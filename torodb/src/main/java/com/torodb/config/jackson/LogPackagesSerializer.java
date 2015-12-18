/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.config.jackson;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.torodb.config.model.generic.LogLevel;
import com.torodb.config.model.generic.LogPackages;
import com.torodb.config.util.DescriptionFactoryWrapper;

public class LogPackagesSerializer extends JsonSerializer<LogPackages> {
	@Override
	public void serialize(LogPackages value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonProcessingException {
		jgen.writeStartObject();

		serializeFields(value, jgen);

		jgen.writeEndObject();
	}

	private void serializeFields(LogPackages value, JsonGenerator jgen) throws IOException {
		for (Map.Entry<String, LogLevel> logPackage : value.entrySet()) {
			jgen.writeStringField(logPackage.getKey(), logPackage.getValue().name());
		}
	}

	public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType type) throws JsonMappingException {
		if (!(visitor instanceof DescriptionFactoryWrapper)) {
			super.acceptJsonFormatVisitor(visitor, type);
		}
	}
}