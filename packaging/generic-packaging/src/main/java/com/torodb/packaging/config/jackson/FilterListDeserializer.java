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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.torodb.packaging.config.model.protocol.mongo.FilterList;

public class FilterListDeserializer extends JsonDeserializer<FilterList> {
	@Override
	public FilterList deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
	    FilterList filterList = new FilterList();

		JsonNode node = jp.getCodec().readTree(jp);
		
		Iterator<Entry<String, JsonNode>> databaseEntriesIterator = node.fields();
		while (databaseEntriesIterator.hasNext()) {
			Entry<String, JsonNode> databaseEntry = databaseEntriesIterator.next();
			
			List<String> collections = new ArrayList<>();
			if (databaseEntry.getValue() instanceof ArrayNode) {
			    ArrayNode collectionArray = (ArrayNode) databaseEntry.getValue();
			    
    	        Iterator<JsonNode> collectionEntriesIterator = collectionArray.elements();
    	        while (collectionEntriesIterator.hasNext()) {
    	            JsonNode collection = collectionEntriesIterator.next();
    	            collections.add(collection.asText());
    	        }
			}
	        
			filterList.put(databaseEntry.getKey(), collections);
		}
		
		return filterList;
	}
}