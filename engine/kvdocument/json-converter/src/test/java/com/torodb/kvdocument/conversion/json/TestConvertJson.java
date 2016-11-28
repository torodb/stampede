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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvInstant;
import com.torodb.kvdocument.values.KvMongoObjectId;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestConvertJson {

  private JsonParser parser;

  @Parameterized.Parameters
  public static Collection<JsonParser> parsers() {
    return Arrays.asList(new JacksonJsonParser(), new GsonJsonParser());
  }

  public TestConvertJson(JsonParser parser) {
    this.parser = parser;
  }

  @Test
  public void parseEmptyJson() {
    KvDocument doc = parser.createFromResource("Empty.json");
    assertEquals(0, doc.size());
  }

  @Test
  public void parseField() {
    KvDocument doc = parser.createFromResource("OneField.json");
    assertEquals(1, doc.size());
    assertTrue(doc.containsKey("name"));
    assertEquals("John", doc.get("name").getValue());
  }

  @Test
  public void parseMultipleFields() {
    KvDocument doc = parser.createFromResource("MultipleFields.json");
    assertEquals(2, doc.size());
    assertTrue(doc.containsKey("name"));
    assertEquals("John", doc.get("name").getValue());
    assertTrue(doc.containsKey("surename"));
    assertEquals("Snow", doc.get("surename").getValue());
  }

  @Test
  public void parse_id() {
    KvDocument doc = parser.createFromResource("_idField.json");
    assertTrue(doc.containsKey("_id"));
    KvValue<?> id = doc.get("_id");
    assertTrue(id instanceof KvMongoObjectId);
    assertEquals("55129FF25916F02D31387E1C", id.toString());
  }

  @Test
  public void parseDate() {
    KvDocument doc = parser.createFromResource("DateField.json");
    assertTrue(doc.containsKey("creation"));
    KvValue<?> creation = doc.get("creation");
    assertTrue(creation instanceof KvInstant);
  }

  @Test
  public void parseNullField() {
    KvDocument doc = parser.createFromResource("NullField.json");
    assertTrue(doc.containsKey("age"));
    assertEquals(KvNull.getInstance(), doc.get("age"));
  }

  @Test
  public void parseSubDocument() {
    KvDocument doc = parser.createFromResource("SubDocument.json");
    assertTrue(doc.containsKey("address"));
    assertTrue(doc.get("address") instanceof KvDocument);
  }

  @Test
  public void parseEmptyArray() {
    KvDocument doc = parser.createFromResource("EmptyArray.json");
    assertTrue(doc.containsKey("department"));
    assertTrue(doc.get("department") instanceof KvArray);
    assertTrue(((KvArray) doc.get("department")).size() == 0);
  }

  @Test
  public void parseArrayWithScalar() {
    KvDocument doc = parser.createFromResource("ArrayWithScalar.json");
    KvArray array = (KvArray) doc.get("day");
    assertEquals(1, array.size());
    KvValue<?> kvValue = array.get(0);
    assertEquals("monday", kvValue.getValue());
  }

  @Test
  public void parseArrayWithDocument() {
    KvDocument doc = parser.createFromResource("ArrayWithDocument.json");
    KvArray array = (KvArray) doc.get("department");
    assertEquals(1, array.size());
    assertTrue(array.get(0) instanceof KvDocument);
  }

  @Test
  public void parseMultiArrayEmpty() {
    KvDocument doc = parser.createFromResource("MultiArrayEmpty.json");
    KvArray array = (KvArray) doc.get("months");
    assertEquals(1, array.size());
    assertTrue(array.get(0) instanceof KvArray);
    KvArray inner = (KvArray) array.get(0);
    assertEquals(0, inner.size());
  }

  @Test
  public void parseMultiArrayWithValue() {
    KvDocument doc = parser.createFromResource("MultiArray.json");
    KvArray array = (KvArray) doc.get("months");
    KvArray inner = (KvArray) array.get(0);
    assertEquals("April", inner.get(0).getValue());
  }

}
