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

package com.torodb.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.DefaultIdentifierFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IdentifierFactoryImplTest {

  private DefaultIdentifierFactory identifierFactory;
  private TableRefFactory tableRefFactory = new TableRefFactoryImpl();

  @Before
  public void setUp() throws Exception {
    this.identifierFactory = new DefaultIdentifierFactory(new MockIdentifierInterface());
  }

  @Test
  public void emptyDatabaseToIdentifierTest() {
    ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
    String identifier = identifierFactory.toDatabaseIdentifier(metaSnapshot, "");
    Assert.assertEquals("", identifier);
  }

  @Test
  public void unallowedDatabaseToIdentifierTest() {
    ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
    String identifier = identifierFactory.toDatabaseIdentifier(metaSnapshot, "unallowed_schema");
    Assert.assertEquals("_unallowed_schema", identifier);
  }

  @Test
  public void databaseToIdentifierTest() {
    ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
    String identifier = identifierFactory.toDatabaseIdentifier(metaSnapshot, "database");
    Assert.assertEquals("database", identifier);
  }

  @Test
  public void long128DatabaseToIdentifierTest() {
    ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
    String identifier = identifierFactory.toDatabaseIdentifier(metaSnapshot,
        "database_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
    Assert.assertEquals(
        "database_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long",
        identifier);
  }

  @Test
  public void longForCounterDatabaseToIdentifierTest() {
    ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder().build();
    String identifier = identifierFactory.toDatabaseIdentifier(metaSnapshot,
        "database_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
    Assert.assertEquals(
        "database_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_1",
        identifier);
  }

  @Test
  public void longForCounterWithCollisionCharacterDatabaseToIdentifierTest() {
    ImmutableMetaSnapshot metaSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase.Builder("database_collider",
            "database_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_1"))
        .build();
    String identifier = identifierFactory.toDatabaseIdentifier(metaSnapshot,
        "database_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long");
    Assert.assertEquals(
        "database_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_2",
        identifier);
  }

  @Test
  public void emptyCollectionDocPartRootToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase, "", createTableRef());
    Assert.assertEquals("", identifier);
  }

  @Test
  public void unallowedCollectionDocPartRootToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase, "unallowed_table",
        createTableRef());
    Assert.assertEquals("_unallowed_table", identifier);
  }

  @Test
  public void docPartRootToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase, "collecti",
        createTableRef());
    Assert.assertEquals("collecti", identifier);
  }

  @Test
  public void docPartObjectChildToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase, "collecti",
        createTableRef("object"));
    Assert.assertEquals("collecti_object", identifier);
  }

  @Test
  public void docPartArrayChildToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase, "collecti",
        createTableRef("array"));
    Assert.assertEquals("collecti_array", identifier);
  }

  @Test
  public void docPartArrayInArrayChildToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase, "collecti",
        createTableRef("array", "2"));
    Assert.assertEquals("collecti_array$2", identifier);
  }

  @Test
  public void docPartObjectArrayInArrayObjectToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase, "collecti",
        createTableRef("object", "array", "2", "object"));
    Assert.assertEquals("collecti_object_array$2_object", identifier);
  }

  @Test
  public void emptyDocPartToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase, "collecti",
        createTableRef(""));
    Assert.assertEquals("collecti_", identifier);
  }

  @Test
  public void long128DocPartToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase,
        "collecti",
        createTableRef(
            "long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long"));
    Assert.assertEquals(
        "collecti_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long",
        identifier);
  }

  @Test
  public void longForCounterDocPartToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase,
        "collecti",
        createTableRef(
            "long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long"));
    Assert.assertEquals(
        "collecti_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_1",
        identifier);
  }

  @Test
  public void longForCounterWithCollisionCharacterDocPartToIdentifierTest() {
    ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database", "database")
        .put(new ImmutableMetaCollection.Builder("collecti", "collecti")
            .put(new ImmutableMetaDocPart.Builder(createTableRef(),
                "collecti_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_1")
                .build())
            .build())
        .build();
    String identifier = identifierFactory.toDocPartIdentifier(metaDatabase,
        "collecti",
        createTableRef(
            "long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long"));
    Assert.assertEquals(
        "collecti_long_long_long_long_long_long_long_long_long_long_longong_long_long_long_long_long_long_long_long_long_long_long_long_2",
        identifier);
  }

  @Test
  public void emptyFieldToIdentifierTest() {
    ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(createTableRef(),
        "docpart")
        .build();
    String identifier = identifierFactory.toFieldIdentifier(metaDocPart, "", FieldType.STRING);
    Assert.assertEquals("_s", identifier);
  }

  @Test
  public void unallowedFieldToIdentifierTest() {
    ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(createTableRef(),
        "docpart")
        .build();
    String identifier = identifierFactory.toFieldIdentifier(metaDocPart, "unallowed_column",
        FieldType.STRING);
    Assert.assertEquals("_unallowed_column_s", identifier);
  }

  @Test
  public void fieldToIdentifierTest() {
    ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(createTableRef(),
        "docpart")
        .build();
    String identifier = identifierFactory.toFieldIdentifier(metaDocPart, "field", FieldType.STRING);
    Assert.assertEquals("field_s", identifier);
  }

  @Test
  public void long128FieldToIdentifierTest() {
    ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(createTableRef(),
        "docpart")
        .build();
    String identifier = identifierFactory.toFieldIdentifier(metaDocPart,
        "field__long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long",
        FieldType.STRING);
    Assert.assertEquals(
        "field__long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_s",
        identifier);
  }

  @Test
  public void longForCounterFieldToIdentifierTest() {
    ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(createTableRef(),
        "docpart")
        .build();
    String identifier = identifierFactory.toFieldIdentifier(metaDocPart,
        "field____long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long",
        FieldType.STRING);
    Assert.assertEquals(
        "field____long_long_long_long_long_long_long_long_long_long_lonng_long_long_long_long_long_long_long_long_long_long_long_long_1_s",
        identifier);
  }

  @Test
  public void longForCounterWithCollisionCharacterFieldToIdentifierTest() {
    ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(createTableRef(),
        "docpart")
        .put(new ImmutableMetaField("field_collider",
            "field____long_long_long_long_long_long_long_long_long_long_lonng_long_long_long_long_long_long_long_long_long_long_long_long_1_s",
            FieldType.STRING))
        .build();
    String identifier = identifierFactory.toFieldIdentifier(metaDocPart,
        "field____long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long",
        FieldType.STRING);
    Assert.assertEquals(
        "field____long_long_long_long_long_long_long_long_long_long_lonng_long_long_long_long_long_long_long_long_long_long_long_long_2_s",
        identifier);
  }

  private TableRef createTableRef(String... names) {
    TableRef tableRef = tableRefFactory.createRoot();

    for (String name : names) {
      try {
        int index = Integer.parseInt(name);
        tableRef = tableRefFactory.createChild(tableRef, index);
      } catch (NumberFormatException ex) {
        tableRef = tableRefFactory.createChild(tableRef, name);
      }
    }

    return tableRef;
  }
}
