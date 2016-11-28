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

package com.torodb.kvdocument.conversion.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.torodb.kvdocument.values.KvDocument;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class JacksonJsonParser implements JsonParser {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final MapToKvValueConverter converter = new MapToKvValueConverter();
  private static final TypeReference<List<HashMap<String, Object>>> typeReference =
      new TypeReference<List<HashMap<String, Object>>>() {
  };

  @Override
  public KvDocument createFromJson(String json) {
    try {
      return converter.convert(mapper.readValue(json, HashMap.class));
    } catch (IOException e) {
      throw new RuntimeException("Unparseable document: " + json);
    }
  }

  @Override
  public List<KvDocument> createListFromJson(String json) {
    try {
      return converter.convert((List<Map<String, Object>>) mapper.readValue(json, typeReference));
    } catch (IOException e) {
      throw new RuntimeException("Unparseable document: " + json);
    }
  }

  @Override
  public KvDocument createFrom(InputStream is) {
    try {
      return converter.convert(mapper.readValue(is, HashMap.class));
    } catch (IOException e) {
      throw new RuntimeException("Unparseable document from InputStream", e);
    }
  }

  @Override
  public List<KvDocument> createListFrom(InputStream is) {
    try {
      return converter.convert((List<Map<String, Object>>) mapper.readValue(is, typeReference));
    } catch (IOException e) {
      throw new RuntimeException("Unparseable document from InputStream", e);
    }
  }

  @Override
  public KvDocument createFromResource(String name) {
    return createFrom(this.getClass().getClassLoader().getResourceAsStream(name));
  }

  @Override
  public List<KvDocument> createListFromResource(String name) {
    return createListFrom(this.getClass().getClassLoader().getResourceAsStream(name));
  }

}
