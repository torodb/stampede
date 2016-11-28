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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.torodb.packaging.config.model.protocol.mongo.FilterList;
import com.torodb.packaging.config.model.protocol.mongo.FilterList.IndexFilter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FilterListDeserializer extends JsonDeserializer<FilterList> {

  @Override
  @SuppressFBWarnings("REC_CATCH_EXCEPTION")
  public FilterList deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
      JsonProcessingException {
    FilterList filterList = new FilterList();

    JsonNode node = jp.getCodec().readTree(jp);

    if (node instanceof ObjectNode) {
      Iterator<Entry<String, JsonNode>> databaseEntriesIterator = node.fields();
      while (databaseEntriesIterator.hasNext()) {
        Entry<String, JsonNode> databaseEntry = databaseEntriesIterator.next();

        try {
          Map<String, List<IndexFilter>> collections = new HashMap<>();
          if (databaseEntry.getValue() instanceof ObjectNode) {
            readCollectionObject(jp, (ObjectNode) databaseEntry.getValue(), collections);
          } else if (databaseEntry.getValue() instanceof ArrayNode) {
            ArrayNode collectionsArray = (ArrayNode) databaseEntry.getValue();

            Iterator<JsonNode> collectionsIterator = collectionsArray.elements();
            int position = 0;
            while (collectionsIterator.hasNext()) {
              try {
                JsonNode collection = collectionsIterator.next();
                if (collection instanceof ObjectNode) {
                  readCollectionObject(jp, (ObjectNode) collection, collections);
                } else if (collection instanceof ArrayNode) {
                  throw new JsonMappingException("wrong filter format: collection value inside "
                      + "database array can not be an array",
                      jp.getCurrentLocation());
                } else {
                  collections.put(collection.asText(), new ArrayList<>());
                }
                position++;
              } catch (Exception e) {
                throw JsonMappingException.wrapWithPath(e, collections, position);
              }
            }
          }

          filterList.put(databaseEntry.getKey(), collections);
        } catch (Exception e) {
          throw JsonMappingException.wrapWithPath(e, filterList, databaseEntry.getKey());
        }
      }
    } else {
      throw new JsonMappingException("wrong filter format: filter list was not an object", jp
          .getCurrentLocation());
    }

    return filterList;
  }

  @SuppressFBWarnings("REC_CATCH_EXCEPTION")
  private void readCollectionObject(JsonParser jp, ObjectNode collection,
      Map<String, List<IndexFilter>> collections)
      throws JsonProcessingException, JsonMappingException {
    Iterator<Entry<String, JsonNode>> collectionEntriesIterator = collection.fields();
    while (collectionEntriesIterator.hasNext()) {
      List<IndexFilter> indexFilters = new ArrayList<>();
      Map.Entry<String, JsonNode> collectionEntry = collectionEntriesIterator.next();
      try {
        if (collectionEntry.getValue() instanceof ObjectNode) {
          readIndexFilter(jp, collectionEntry.getValue(), indexFilters);
        } else if (collectionEntry.getValue() instanceof ArrayNode) {
          Iterator<JsonNode> indexFiltersIterator = collectionEntry.getValue().elements();
          int position = 0;
          while (indexFiltersIterator.hasNext()) {
            try {
              JsonNode indexFilter = indexFiltersIterator.next();
              if (indexFilter instanceof ObjectNode) {
                readIndexFilter(jp, indexFilter, indexFilters);
              } else {
                throw new JsonMappingException("wrong filter format: index filter should be an "
                    + "object", jp.getCurrentLocation());
              }
              position++;
            } catch (Exception e) {
              throw JsonMappingException.wrapWithPath(e, indexFilters, position);
            }
          }
        }
        collections.put(collectionEntry.getKey(), indexFilters);
      } catch (Exception e) {
        throw JsonMappingException.wrapWithPath(e, collections, collectionEntry.getKey());
      }
    }
  }

  private void readIndexFilter(JsonParser jp, JsonNode indexFilter, List<IndexFilter> indexFilters)
      throws JsonProcessingException {
    ObjectMapper mapper = (ObjectMapper) jp.getCodec();
    indexFilters.add(mapper.treeToValue(indexFilter, IndexFilter.class));
  }
}
