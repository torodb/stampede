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

package com.torodb.backend.converters.jooq;

import com.google.common.collect.Sets;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.model.DefaultNamedToroIndex;
import com.torodb.core.model.IndexedAttributes;
import com.torodb.core.model.IndexedAttributes.IndexType;
import com.torodb.core.model.NamedToroIndex;
import org.jooq.Converter;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonWriter;

public class ToroIndexConverter implements Converter<String, NamedToroIndex> {

  private static final long serialVersionUID = 1L;

  private final String databaseName;
  private final String collectionName;

  private static final String ATTS_KEY = "atts";
  private static final String UNIQUE_KEY = "unique";
  private static final String NAME_KEY = "key";
  private static final String DESCENDING = "desc";

  public ToroIndexConverter(String databaseName, String collectionName) {
    this.databaseName = databaseName;
    this.collectionName = collectionName;
  }

  @Override
  public NamedToroIndex from(String databaseObject) {
    JsonReader reader = Json.createReader(new StringReader(databaseObject));
    JsonObject object = reader.readObject();

    IndexedAttributes.Builder builder = new IndexedAttributes.Builder();
    JsonArray attsArray = object.getJsonArray(ATTS_KEY);
    Set<Integer> descendingAttPos;
    if (object.containsKey(DESCENDING)) {
      JsonArray descArray = object.getJsonArray(DESCENDING);
      descendingAttPos = Sets.newHashSetWithExpectedSize(descArray.size());
      for (int i = 0; i < descArray.size(); i++) {
        descendingAttPos.add(descArray.getInt(i));
      }
    } else {
      descendingAttPos = Collections.emptySet();
    }

    for (int i = 0; i < attsArray.size(); i++) {
      String att = attsArray.getString(i);
      AttributeReference attRef = parseAttRef(att);
      if (descendingAttPos.contains(i)) {
        builder.addAttribute(attRef, IndexType.desc);
      } else {
        builder.addAttribute(attRef, IndexType.asc);
      }
    }

    return new DefaultNamedToroIndex(
        object.getString(NAME_KEY),
        builder.build(),
        databaseName,
        collectionName,
        object.getBoolean(UNIQUE_KEY, false)
    );
  }

  @Override
  public String to(NamedToroIndex userObject) {
    JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
    objectBuilder.add(NAME_KEY, userObject.getName());
    if (userObject.isUnique()) {
      objectBuilder.add(UNIQUE_KEY, true);
    }

    JsonArrayBuilder attsBuilder = Json.createArrayBuilder();
    JsonArrayBuilder descBuilder = Json.createArrayBuilder();
    int attPosition = 0;
    boolean hasDescending = false;
    for (Map.Entry<AttributeReference, IndexType> entry : userObject.getAttributes().entrySet()) {
      attsBuilder.add(entry.getKey().toString());

      if (IndexType.desc.equals(entry.getValue())) {
        descBuilder.add(attPosition);
        hasDescending = true;
      }
      attPosition++;
    }
    objectBuilder.add(ATTS_KEY, attsBuilder);
    if (hasDescending) {
      objectBuilder.add(DESCENDING, descBuilder);
    }

    StringWriter stringWriter = new StringWriter(200);

    JsonWriter jsonWriter = Json.createWriter(stringWriter);
    jsonWriter.writeObject(objectBuilder.build());
    return stringWriter.toString();
  }

  @Override
  public Class<String> fromType() {
    return String.class;
  }

  @Override
  public Class<NamedToroIndex> toType() {
    return NamedToroIndex.class;
  }

  private AttributeReference parseAttRef(String key) {
    //TODO: check attributes with '\.' characters
    //TODO: Check attributes references with array keys
    StringTokenizer tk = new StringTokenizer(key, ".");
    AttributeReference.Builder attRefBuilder = new AttributeReference.Builder();
    while (tk.hasMoreTokens()) {
      attRefBuilder.addObjectKey(tk.nextToken());
    }
    return attRefBuilder.build();
  }
}
