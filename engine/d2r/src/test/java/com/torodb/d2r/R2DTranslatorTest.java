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

import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvValue;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class R2DTranslatorTest {

  private TableRefFactory fact = new TableRefFactoryImpl();
  private TableRef rootRef = fact.createRoot();

  private static final boolean IsArray = true;
  private static final boolean IsDocument = false;

  /*
   * Document: { "name" : "jero" }
   */
  @Test
  public void readSimpleDocument() {
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addRow(1, null, 1, null, "jero");
    DocPartResult root = builder.getResultSet();

    List<DocPartResult> lst = Collections.singletonList(root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    Collection<ToroDocument> readedDocuments = r2dTranslator.translate(lst.iterator());
    assertEquals(1, readedDocuments.size());
    KvDocument doc = readedDocuments.iterator().next().getRoot();
    assertEquals("jero", doc.get("name").getValue());
  }

  /*
   * Document: { "name" : "jero", "address" : null }
   */
  @Test
  public void readDocumentWithNullField() {
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addMetaField("address", "address_s", FieldType.STRING);
    builder.addMetaField("address", "address_n", FieldType.NULL);
    builder.addRow(1, null, 1, null, "jero", null, true);
    MockedDocPartResult root = builder.getResultSet();

    List<DocPartResult> lst = Collections.singletonList(root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    Collection<ToroDocument> readedDocuments = r2dTranslator.translate(lst.iterator());
    assertEquals(1, readedDocuments.size());
    KvDocument doc = readedDocuments.iterator().next().getRoot();
    assertEquals("jero", doc.get("name").getValue());
    assertEquals(KvNull.getInstance(), doc.get("address"));
  }

  /*
   * Document: { "name" : "jero" }
   */
  @Test
  public void readDocumentWithOutPresentFields() {
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addMetaField("address", "address_s", FieldType.STRING);
    builder.addRow(1, null, 1, null, "jero", null);
    MockedDocPartResult root = builder.getResultSet();

    List<DocPartResult> lst = Collections.singletonList(root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    Collection<ToroDocument> readedDocuments = r2dTranslator.translate(lst.iterator());
    assertEquals(1, readedDocuments.size());
    KvDocument doc = readedDocuments.iterator().next().getRoot();
    assertEquals("jero", doc.get("name").getValue());
    assertNull(doc.get("address"));
  }

  /*
   * Document: { "name" : "jero", "address" : "my home", "age" : 25 }
   */
  @Test
  public void readDocumentWithMultipleFields() {
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addMetaField("address", "address_s", FieldType.STRING);
    builder.addMetaField("age", "age_i", FieldType.INTEGER);
    builder.addRow(1, null, 1, null, "jero", "my home", 25);
    MockedDocPartResult root = builder.getResultSet();

    List<DocPartResult> lst = Collections.singletonList(root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    Collection<ToroDocument> readedDocuments = r2dTranslator.translate(lst.iterator());
    assertEquals(1, readedDocuments.size());
    KvDocument doc = readedDocuments.iterator().next().getRoot();
    assertEquals("jero", doc.get("name").getValue());
    assertEquals("my home", doc.get("address").getValue());
    assertEquals(25, doc.get("age").getValue());
  }

  /*
   * Document 1 { "name" : "jero" } Document 2 { "name" : "john" }
   */
  @Test
  public void readMultipleDocument() {
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addRow(1, null, 1, null, "jero");
    builder.addRow(2, null, 2, null, "john");
    MockedDocPartResult root = builder.getResultSet();

    List<DocPartResult> lst = Collections.singletonList(root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    ToroDocument[] readedDocuments = r2dTranslator.translate(lst.iterator()).toArray(
        new ToroDocument[0]);
    assertEquals(2, readedDocuments.length);

    KvDocument doc1 = readedDocuments[0].getRoot();
    assertEquals("jero", doc1.get("name").getValue());

    KvDocument doc2 = readedDocuments[1].getRoot();
    assertEquals("john", doc2.get("name").getValue());

  }

  /*
   * Document: { "name" : "jero", "address" : { "street" : "myhouse" } }
   */
  @Test
  public void readTwoLevelDocument() {
    /*
     * Root Level
     */
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addMetaField("address", "address_e", FieldType.CHILD);
    builder.addRow(1, null, 1, null, "jero", IsDocument);
    MockedDocPartResult root = builder.getResultSet();

    /*
     * Second Level
     */
    TableRef secondRef = fact.createChild(rootRef, "address");
    MetaDocPartBuilder secondBuilder = new MetaDocPartBuilder(secondRef);
    secondBuilder.addMetaField("street", "street_s", FieldType.STRING);
    secondBuilder.addRow(1, 1, 20, null, "myhouse");
    MockedDocPartResult secondLevel = secondBuilder.getResultSet();

    List<DocPartResult> lst = Lists.newArrayList(secondLevel, root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    Collection<ToroDocument> readedDocuments = r2dTranslator.translate(lst.iterator());
    assertEquals(1, readedDocuments.size());
    KvDocument doc = readedDocuments.iterator().next().getRoot();
    assertEquals("jero", doc.get("name").getValue());
    KvValue<?> kvValue = doc.get("address");
    assertTrue(kvValue instanceof KvDocument);
  }

  /*
   * Document: { "name" : "jero", "numbers" : [4, 8, 15, 16] }
   */
  @Test
  public void readInnerArray() {
    /*
     * Root Level
     */
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addMetaField("numbers", "numbers_e", FieldType.CHILD);
    builder.addRow(1, null, 1, null, "jero", IsArray);
    MockedDocPartResult root = builder.getResultSet();

    /*
     * Second Level
     */
    TableRef secondRef = fact.createChild(rootRef, "numbers");
    MetaDocPartBuilder secondBuilder = new MetaDocPartBuilder(secondRef);
    secondBuilder.addMetaScalar("v_i", FieldType.INTEGER);
    secondBuilder.addRow(1, 1, 20, 0, 4);
    secondBuilder.addRow(1, 1, 21, 1, 8);
    secondBuilder.addRow(1, 1, 23, 2, 15);
    secondBuilder.addRow(1, 1, 24, 3, 16);
    MockedDocPartResult secondLevel = secondBuilder.getResultSet();

    List<DocPartResult> lst = Lists.newArrayList(secondLevel, root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    Collection<ToroDocument> readedDocuments = r2dTranslator.translate(lst.iterator());
    assertEquals(1, readedDocuments.size());
    KvDocument doc = readedDocuments.iterator().next().getRoot();
    assertEquals("jero", doc.get("name").getValue());
    KvValue<?> kvValue = doc.get("numbers");
    assertTrue(kvValue instanceof KvArray);
    KvArray array = (KvArray) kvValue;
    assertEquals(4, array.size());
    assertEquals(4, array.get(0).getValue());
    assertEquals(8, array.get(1).getValue());
    assertEquals(15, array.get(2).getValue());
    assertEquals(16, array.get(3).getValue());
  }

  /*
   * Document: { "name" : "jero", "numbers" : [666, [4,8, 15, 16]] }
   */
  @Test
  public void readTwoInnerArray() {
    int did = 1;
    /*
     * Root Level
     */
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addMetaField("numbers", "numbers_e", FieldType.CHILD);

    builder.addRow(did, null, did, null, "jero", IsArray);
    MockedDocPartResult root = builder.getResultSet();

    /*
     * Second Level
     */
    int pid = did;
    TableRef secondRef = fact.createChild(rootRef, "numbers");
    MetaDocPartBuilder secondBuilder = new MetaDocPartBuilder(secondRef);
    secondBuilder.addMetaScalar("v_i", FieldType.INTEGER);
    secondBuilder.addMetaScalar("v_e", FieldType.CHILD);

    secondBuilder.addRow(did, pid, 20, 0, 666, null);
    secondBuilder.addRow(did, pid, 21, 1, null, IsArray);
    MockedDocPartResult secondLevel = secondBuilder.getResultSet();

    /*
     * Third Level
     */
    int pid1 = 21;
    TableRef thirdRef = fact.createChild(secondRef, 2);
    MetaDocPartBuilder thirdBuilder = new MetaDocPartBuilder(thirdRef);
    thirdBuilder.addMetaScalar("v_i", FieldType.INTEGER);

    thirdBuilder.addRow(did, pid1, 30, 0, 4);
    thirdBuilder.addRow(did, pid1, 31, 1, 8);
    thirdBuilder.addRow(did, pid1, 33, 2, 15);
    thirdBuilder.addRow(did, pid1, 34, 3, 16);
    MockedDocPartResult thirdLevel = thirdBuilder.getResultSet();

    List<DocPartResult> lst = Lists.newArrayList(
        thirdLevel,
        secondLevel,
        root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    Collection<ToroDocument> readedDocuments = r2dTranslator.translate(lst.iterator());
    assertEquals(1, readedDocuments.size());
    KvDocument doc = readedDocuments.iterator().next().getRoot();
    assertEquals("jero", doc.get("name").getValue());
    KvValue<?> kvValue = doc.get("numbers");
    assertTrue(kvValue instanceof KvArray);
    KvArray array = (KvArray) kvValue;
    assertEquals(2, array.size());
    assertEquals(666, array.get(0).getValue());
    KvValue<?> kvValueSecond = array.get(1);
    assertNotNull(kvValueSecond);
    assertTrue(kvValueSecond instanceof KvArray);
    KvArray array2 = (KvArray) kvValueSecond;
    assertEquals(4, array2.size());
    assertEquals(4, array2.get(0).getValue());
    assertEquals(8, array2.get(1).getValue());
    assertEquals(15, array2.get(2).getValue());
    assertEquals(16, array2.get(3).getValue());
  }

  /*
   * Document: { "name" : "jero", "numbers" : [ 666, { "address": "myhome" } ] }
   */
  @Test
  public void readDocumentInArray() {
    int did = 1;
    /*
     * Root Level
     */
    MetaDocPartBuilder builder = new MetaDocPartBuilder(rootRef);
    builder.addMetaField("name", "name_s", FieldType.STRING);
    builder.addMetaField("numbers", "numbers_e", FieldType.CHILD);

    builder.addRow(did, null, did, null, "jero", IsArray);
    MockedDocPartResult root = builder.getResultSet();

    /*
     * Second Level
     */
    int pid = did;
    TableRef secondRef = fact.createChild(rootRef, "numbers");
    MetaDocPartBuilder secondBuilder = new MetaDocPartBuilder(secondRef);
    secondBuilder.addMetaScalar("v_i", FieldType.INTEGER);
    secondBuilder.addMetaField("address", "address_s", FieldType.STRING);

    secondBuilder.addRow(did, pid, 20, 0, 666, null);
    secondBuilder.addRow(did, pid, 21, 1, null, "myhome");
    MockedDocPartResult secondLevel = secondBuilder.getResultSet();

    List<DocPartResult> lst = Lists.newArrayList(
        secondLevel,
        root);

    R2DTranslator r2dTranslator = new R2DTranslatorImpl();
    Collection<ToroDocument> readedDocuments = r2dTranslator.translate(lst.iterator());
    assertEquals(1, readedDocuments.size());
    KvDocument doc = readedDocuments.iterator().next().getRoot();
    assertEquals("jero", doc.get("name").getValue());
    KvValue<?> kvValue = doc.get("numbers");
    assertTrue(kvValue instanceof KvArray);
    KvArray array = (KvArray) kvValue;
    assertEquals(2, array.size());
    assertEquals(666, array.get(0).getValue());
    KvValue<?> kvValueSecond = array.get(1);
    assertNotNull(kvValueSecond);
    assertTrue(kvValueSecond instanceof KvDocument);
    KvDocument doc2 = (KvDocument) kvValueSecond;
    assertEquals("myhome", doc2.get("address").getValue());
  }
}
