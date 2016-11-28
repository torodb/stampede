/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.packaging.config.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.generic.LogPackages;
import com.torodb.packaging.config.util.DescriptionFactoryWrapper;

import java.io.IOException;
import java.util.Map;

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

  public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType type) throws
      JsonMappingException {
    if (!(visitor instanceof DescriptionFactoryWrapper)) {
      super.acceptJsonFormatVisitor(visitor, type);
    }
  }
}
