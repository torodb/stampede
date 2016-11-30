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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.generic.LogPackages;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

public class LogPackagesDeserializer extends JsonDeserializer<LogPackages> {

  @Override
  public LogPackages deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
      JsonProcessingException {
    LogPackages logPackages = new LogPackages();

    JsonNode node = jp.getCodec().readTree(jp);

    Iterator<Entry<String, JsonNode>> fieldsIterator = node.fields();
    while (fieldsIterator.hasNext()) {
      Entry<String, JsonNode> field = fieldsIterator.next();

      logPackages.put(field.getKey(), LogLevel.valueOf(field.getValue().asText()));
    }

    return logPackages;
  }
}
