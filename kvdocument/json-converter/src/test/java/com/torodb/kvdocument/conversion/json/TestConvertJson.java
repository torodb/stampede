/*
 * MongoWP - KVDocument: Gson Converter
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.kvdocument.conversion.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVInstant;
import com.torodb.kvdocument.values.KVMongoObjectId;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;

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
		KVDocument doc = parser.createFromResource("Empty.json");
		assertEquals(0, doc.size());
	}

	@Test
	public void parseField() {
		KVDocument doc = parser.createFromResource("OneField.json");
		assertEquals(1, doc.size());
		assertTrue(doc.containsKey("name"));
		assertEquals("John", doc.get("name").getValue());
	}

	@Test
	public void parseMultipleFields() {
		KVDocument doc = parser.createFromResource("MultipleFields.json");
		assertEquals(2, doc.size());
		assertTrue(doc.containsKey("name"));
		assertEquals("John", doc.get("name").getValue());
		assertTrue(doc.containsKey("surename"));
		assertEquals("Snow", doc.get("surename").getValue());
	}

	@Test
	public void parse_id() {
		KVDocument doc = parser.createFromResource("_idField.json");
		assertTrue(doc.containsKey("_id"));
		KVValue<?> id = doc.get("_id");
		assertTrue(id instanceof KVMongoObjectId);
		assertEquals("55129FF25916F02D31387E1C", id.toString());
	}

	@Test
	public void parseDate() {
		KVDocument doc = parser.createFromResource("DateField.json");
		assertTrue(doc.containsKey("creation"));
		KVValue<?> creation = doc.get("creation");
		assertTrue(creation instanceof KVInstant);
	}

	@Test
	public void parseNullField() {
		KVDocument doc = parser.createFromResource("NullField.json");
		assertTrue(doc.containsKey("age"));
		assertEquals(KVNull.getInstance(), doc.get("age"));
	}

	@Test
	public void parseSubDocument() {
		KVDocument doc = parser.createFromResource("SubDocument.json");
		assertTrue(doc.containsKey("address"));
		assertTrue(doc.get("address") instanceof KVDocument);
	}

	@Test
	public void parseEmptyArray() {
		KVDocument doc = parser.createFromResource("EmptyArray.json");
		assertTrue(doc.containsKey("department"));
		assertTrue(doc.get("department") instanceof KVArray);
		assertTrue(((KVArray) doc.get("department")).size() == 0);
	}

	@Test
	public void parseArrayWithScalar() {
		KVDocument doc = parser.createFromResource("ArrayWithScalar.json");
		KVArray array = (KVArray) doc.get("day");
		assertEquals(1, array.size());
		KVValue<?> kvValue = array.get(0);
		assertEquals("monday", kvValue.getValue());
	}

	@Test
	public void parseArrayWithDocument() {
		KVDocument doc = parser.createFromResource("ArrayWithDocument.json");
		KVArray array = (KVArray) doc.get("department");
		assertEquals(1, array.size());
		assertTrue(array.get(0) instanceof KVDocument);
	}

	@Test
	public void parseMultiArrayEmpty() {
		KVDocument doc = parser.createFromResource("MultiArrayEmpty.json");
		KVArray array = (KVArray) doc.get("months");
		assertEquals(1, array.size());
		assertTrue(array.get(0) instanceof KVArray);
		KVArray inner = (KVArray) array.get(0);
		assertEquals(0, inner.size());
	}

	@Test
	public void parseMultiArrayWithValue() {
		KVDocument doc = parser.createFromResource("MultiArray.json");
		KVArray array = (KVArray) doc.get("months");
		KVArray inner = (KVArray) array.get(0);
		assertEquals("April", inner.get(0).getValue());
	}

}
