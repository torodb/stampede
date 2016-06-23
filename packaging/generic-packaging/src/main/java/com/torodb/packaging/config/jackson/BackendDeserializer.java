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

package com.torodb.packaging.config.jackson;

import java.io.IOException;
import java.util.Iterator;

import org.bson.json.JsonParseException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.torodb.packaging.config.model.backend.Backend;
import com.torodb.packaging.config.model.backend.BackendImplementation;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.backend.postgres.Postgres;
import com.torodb.core.exceptions.SystemException;

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
			
			if (backendImplementationClass != null) {
                throw new JsonParseException("Found multiples backend implementations but only one is allowed");
			}
			
			backendImplementationNode = node.get(fieldName);
			if ("postgres".equals(fieldName)) {
				backendImplementationClass = Postgres.class;
            } else if ("derby".equals(fieldName)) {
                backendImplementationClass = Derby.class;
			} else {
			    throw new SystemException("Backend " + fieldName + " is not valid.");
			}
		}
		
		if (backendImplementationClass != null) {
			backend.setBackendImplementation(
					jp.getCodec().treeToValue(backendImplementationNode, backendImplementationClass));
		}
		
		return backend;
	}
}