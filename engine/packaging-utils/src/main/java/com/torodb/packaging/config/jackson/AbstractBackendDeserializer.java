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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.exceptions.SystemException;
import com.torodb.packaging.config.model.backend.AbstractBackend;
import com.torodb.packaging.config.model.backend.BackendImplementation;
import org.bson.json.JsonParseException;
import org.jooq.lambda.tuple.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public abstract class AbstractBackendDeserializer<T extends AbstractBackend>
    extends JsonDeserializer<T> {

  private final Supplier<T> backendProvider;
  private final ImmutableMap<String, Tuple2<Class<?>, BiConsumer<T, Object>>> setterMap;

  protected AbstractBackendDeserializer(Supplier<T> backendProvider,
      ImmutableMap<String, Tuple2<Class<?>, BiConsumer<T, Object>>> setterMap) {
    this.backendProvider = backendProvider;
    this.setterMap = setterMap;
  }

  @Override
  public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
      JsonProcessingException {
    T backend = backendProvider.get();
    ObjectMapper mapper = (ObjectMapper) jp.getCodec();
    ObjectNode node = (ObjectNode) jp.getCodec().readTree(jp);

    JsonNode fieldNode = null;
    Class<? extends BackendImplementation> backendImplementationClass = null;
    Iterator<String> fieldNamesIterator = node.fieldNames();
    while (fieldNamesIterator.hasNext()) {
      String fieldName = fieldNamesIterator.next();

      if (backendImplementationClass != null) {
        throw new JsonParseException("Found multiples backend implementations but only one is "
            + "allowed");
      }

      fieldNode = node.get(fieldName);
      if (backend.hasBackendImplementation(fieldName)) {
        backendImplementationClass = backend.getBackendImplementationClass(fieldName);
      } else if (setterMap.containsKey(fieldName)) {
        Object value = mapper.treeToValue(fieldNode, setterMap.get(fieldName).v1());
        setterMap.get(fieldName).v2().accept(backend, value);
      } else {
        throw new SystemException("AbstractBackend " + node.fields().next() + " is not valid.");
      }
    }

    if (backendImplementationClass != null) {
      backend.setBackendImplementation(
          jp.getCodec().treeToValue(fieldNode, backendImplementationClass));
    }

    return backend;
  }

}
