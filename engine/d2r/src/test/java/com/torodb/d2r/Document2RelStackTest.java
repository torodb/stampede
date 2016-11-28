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

import com.torodb.common.util.HexUtils;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.*;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.*;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.kvdocument.conversion.json.JacksonJsonParser;
import com.torodb.kvdocument.conversion.json.JsonParser;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvMongoObjectId;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Document2RelStackTest {

  private final TableRefFactory tableRefFactory = new TableRefFactoryImpl();

  //TODO: Change to final implementation code
  private static final boolean IS_ARRAY = true;
  private static final boolean IS_SUBDOCUMENT = false;
  private static final Integer NO_SEQ = null;
  private static final String ROOT_DOC_NAME = "";

  private JsonParser parser = new JacksonJsonParser();

  private static final String DB1 = "test1";
  private static final String COLLA = "collA";
  private static final String COLLB = "collB";

  private static final String DB2 = "test2";
  private static final String COLLC = "collC";
  private static final String COLLD = "collD";

  private static ImmutableMetaSnapshot currentView = new ImmutableMetaSnapshot.Builder()
      .put(new ImmutableMetaDatabase.Builder(DB1, DB1)
          .put(new ImmutableMetaCollection.Builder(COLLA, COLLA).build())
          .put(new ImmutableMetaCollection.Builder(COLLB, COLLB).build()).build())
      .put(new ImmutableMetaDatabase.Builder(DB2, DB2)
          .put(new ImmutableMetaCollection.Builder(COLLC, COLLC).build())
          .put(new ImmutableMetaCollection.Builder(COLLD, COLLD).build()).build())
      .build();

  private MutableMetaSnapshot mutableSnapshot;

  @Before
  public void setup() {
    MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(currentView);

    try (SnapshotStage snapshot = mvccMetainfoRepository.startSnapshotStage()) {
      mutableSnapshot = snapshot.createMutableSnapshot();
    }
  }

  @Test
  public void emptyDocumentMapsToTable() {
    CollectionData collectionData = parseDocument("EmptyDocument.json");
    assertNotNull(findRootDocPart(collectionData));
  }

  @Test
  public void emptyDocumentCreatesARow() {
    CollectionData collectionData = parseDocument("EmptyDocument.json");
    DocPartData rootDocPart = findRootDocPart(collectionData);
    assertTrue(rootDocPart.iterator().hasNext());
    DocPartRow firstRow = rootDocPart.iterator().next();
    assertNotNull(firstRow);
    assertFalse(firstRow.getFieldValues().iterator().hasNext());
  }

  @Test
  public void rootDocMapsToATableWithEmptyName() {
    CollectionData collectionData = parseDocument("OneField.json");
    assertNotNull(findDocPart(collectionData, ROOT_DOC_NAME));
  }

  @Test
  public void aFieldMapsToAColumn() {
    CollectionData collectionData = parseDocument("OneField.json");

    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow firstRow = rootDocPart.iterator().next();

    int fieldPosition = findFieldPosition(rootDocPart, "name", FieldType.STRING);
    assertTrue(fieldPosition >= 0);
    assertTrue(rootDocPart.iterator().hasNext());
    assertExistFieldValueInPosition(firstRow, 0, "John");
  }

  @Test
  public void multipleFieldsMapsToMultipleColumns() {
    CollectionData collectionData = parseDocument("MultipleFields.json");

    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow firstRow = rootDocPart.iterator().next();

    assertFieldWithValueExists(rootDocPart, firstRow, "name", FieldType.STRING, "John");
    assertFieldWithValueExists(rootDocPart, firstRow, "age", FieldType.INTEGER, 34);
  }

  @Test
  public void nullFieldMapsToNullColumn() {
    CollectionData collectionData = parseDocument("NullField.json");

    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow firstRow = rootDocPart.iterator().next();

    assertFieldWithValueExists(rootDocPart, firstRow, "age", FieldType.NULL, null);
  }

  @Test
  public void emptyArrayMapsToChildColumn() {
    CollectionData collectionData = parseDocument("EmptyArray.json");

    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow firstRow = rootDocPart.iterator().next();

    assertFieldWithValueExists(rootDocPart, firstRow, "department", FieldType.CHILD, IS_ARRAY);
  }

  @Test
  public void arrayCreatesRowInParentTable() {
    CollectionData collectionData = parseDocument("ArrayWithScalar.json");

    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow firstRow = rootDocPart.iterator().next();

    assertFieldWithValueExists(rootDocPart, firstRow, "months", FieldType.CHILD, IS_ARRAY);
  }

  @Test
  public void arrayMapsToNewTable() {
    CollectionData collectionData = parseDocument("ArrayWithScalar.json");
    assertNotNull(findDocPart(collectionData, "months"));
  }

  @Test
  public void scalarInArrayMapsToColumnWithValue() {
    CollectionData collectionData = parseDocument("ArrayWithScalar.json");
    DocPartData monthsDocPart = findDocPart(collectionData, "months");
    DocPartRow firstRow = monthsDocPart.iterator().next();

    assertScalarWithValueExists(monthsDocPart, firstRow, FieldType.INTEGER, 1);
  }

  @Test
  public void subDocumentCreatesRowInParentTable() {
    CollectionData collectionData = parseDocument("SubDocument.json");

    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow firstRow = rootDocPart.iterator().next();

    assertFieldWithValueExists(rootDocPart, firstRow, "address", FieldType.CHILD, IS_SUBDOCUMENT);
  }

  @Test
  public void arrayWithEmptySubdocumentCreatesRow() {
    CollectionData collectionData = parseDocument("ArrayWithEmptyDocument.json");

    DocPartData departmentDocPart = findDocPart(collectionData, "department");
    DocPartRow firstRow = departmentDocPart.iterator().next();
    assertNotNull(firstRow);
    assertFalse(firstRow.getFieldValues().iterator().hasNext());
  }

  @Test
  public void subDocumentMapsToNewTable() {
    CollectionData collectionData = parseDocument("SubDocument.json");
    assertNotNull(findDocPart(collectionData, "address"));
  }

  @Test
  public void subDocumentFiledsMapsIntoNewTable() {
    CollectionData collectionData = parseDocument("SubDocument.json");

    DocPartData addressDocPart = findDocPart(collectionData, "address");
    DocPartRow firstRow = addressDocPart.iterator().next();

    assertFieldWithValueExists(addressDocPart, firstRow, "street", FieldType.STRING, "My Home");
    assertFieldWithValueExists(addressDocPart, firstRow, "zip", FieldType.INTEGER, 28034);
  }

  @Test
  public void subDocumentInArrayMapsToNewTable() {
    CollectionData collectionData = parseDocument("ArrayWithDocument.json");

    DocPartData departmentDocPart = findDocPart(collectionData, "department");
    DocPartRow firstRow = departmentDocPart.iterator().next();

    assertFieldWithValueExists(departmentDocPart, firstRow, "name", FieldType.STRING, "dept1");
  }

  @Test
  public void subDocumentHeterogeneousInArrayMapsToSameTable() {
    CollectionData collectionData = parseDocument("ArrayWithHeteroDocument.json");

    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow rootRow = rootDocPart.iterator().next();

    DocPartData departmentDocPart = findDocPart(collectionData, "department");

    DocPartRow firstRow = findRowSeq(departmentDocPart, rootRow.getRid(), 0);
    assertFieldWithValueExists(departmentDocPart, firstRow, "name", FieldType.STRING, "dept1");

    DocPartRow secondRow = findRowSeq(departmentDocPart, rootRow.getRid(), 1);
    assertFieldWithValueExists(departmentDocPart, secondRow, "code", FieldType.INTEGER, 54);
  }

  @Test
  public void subDocumentAndArrayCanMapToSameTable() {
    CollectionData collectionData = parseDocument("ArrayAndObjectCollision.json");

    DocPartData departmentsDocPart = findDocPart(collectionData, "departments");

    DocPartRow row4Document = findRowSeq(departmentsDocPart, 0);
    assertFieldWithValueExists(departmentsDocPart, row4Document, "dept", FieldType.CHILD,
        IS_SUBDOCUMENT);

    DocPartRow row4Array = findRowSeq(departmentsDocPart, 1);
    assertFieldWithValueExists(departmentsDocPart, row4Array, "dept", FieldType.CHILD, IS_ARRAY);

    DocPartData deptDocPart = findDocPart(collectionData, "departments.dept");

    DocPartRow rowDocument = findRowSeq(deptDocPart, row4Document.getRid(), NO_SEQ);
    assertFieldWithValueExists(deptDocPart, rowDocument, "name", FieldType.STRING, "dept1");

    DocPartRow firstRowArray = findRowSeq(deptDocPart, row4Array.getRid(), 0);
    assertFieldWithValueExists(deptDocPart, firstRowArray, "name", FieldType.STRING, "dept2");

    DocPartRow secondRowArray = findRowSeq(deptDocPart, row4Array.getRid(), 1);
    assertFieldWithValueExists(deptDocPart, secondRowArray, "name", FieldType.STRING, "dept3");
  }

  @Test
  public void arrayInArrayMapsToNewTable() {
    CollectionData collectionData = parseDocument("MultiArray.json");

    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow firstRow = rootDocPart.iterator().next();
    assertFieldWithValueExists(rootDocPart, firstRow, "months", FieldType.CHILD, IS_ARRAY);

    DocPartData monthsDocPart = findDocPart(collectionData, "months");
    DocPartRow firstRowMonths = monthsDocPart.iterator().next();
    assertScalarWithValueExists(monthsDocPart, firstRowMonths, FieldType.CHILD, IS_ARRAY);

    DocPartData subArrayDocPart = findDocPart(collectionData, "months.$2");
    assertNotNull(subArrayDocPart);

    DocPartRow firstRowSubArray = findRowSeq(subArrayDocPart, firstRow.getRid(), 0);
    assertNotNull(firstRowSubArray);
    int fieldSubArray = findScalarPosition(subArrayDocPart, FieldType.INTEGER);
    assertTrue(fieldSubArray >= 0);
    assertExistScalarValueInPosition(firstRowSubArray, fieldSubArray, 1);
  }

  @Test
  public void emptyArrayInArrayMapsToATable() {
    CollectionData collectionData = parseDocument("MultiArrayEmpty.json");

    DocPartData monthsDocPart = findDocPart(collectionData, "months");
    DocPartRow firstRow = monthsDocPart.iterator().next();

    assertScalarWithValueExists(monthsDocPart, firstRow, FieldType.CHILD, IS_ARRAY);

    DocPartData subArrayDocPart = findDocPart(collectionData, "months.$2");
    assertNotNull(subArrayDocPart);
  }

  @Test
  public void mapFieldTypes() {
    CollectionData collectionData = parseDocument("FieldTypes.json");
    DocPartData rootDocPart = findRootDocPart(collectionData);
    DocPartRow firstRow = rootDocPart.iterator().next();

    assertFieldWithValueExists(rootDocPart, firstRow, "_id", FieldType.MONGO_OBJECT_ID, HexUtils
        .hex2Bytes("5298a5a03b3f4220588fe57c"));
    assertFieldWithValueExists(rootDocPart, firstRow, "null", FieldType.NULL, null);
    assertFieldWithValueExists(rootDocPart, firstRow, "boolean", FieldType.BOOLEAN, true);
    assertFieldWithValueExists(rootDocPart, firstRow, "integer", FieldType.INTEGER, 10);
    assertFieldWithValueExists(rootDocPart, firstRow, "double", FieldType.DOUBLE, 10.2);
    assertFieldWithValueExists(rootDocPart, firstRow, "string", FieldType.STRING, "john");
    assertFieldWithValueExists(rootDocPart, firstRow, "long", FieldType.LONG, 10020202020L);
    DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    assertFieldWithValueExists(rootDocPart, firstRow, "date", FieldType.INSTANT, Instant.from(sdf
        .parse("2015-06-18T16:43:58.967Z")));
  }

  @Test
  public void nodesWithDifferentFieldsHasSameNumberOfFieldsInDocPartRow() {
    CollectionData collectionData = parseDocument("NodesWithDifferentFields.json");
    DocPartData docPart = findDocPart(collectionData, "department");
    int fieldsNumber = (int) docPart.getMetaDocPart().streamFields().count();
    Iterator<DocPartRow> it = docPart.iterator();
    DocPartRow first = it.next();
    DocPartRow second = it.next();
    assertEquals(fieldsNumber, countFields(second));
    assertEquals(countFields(first), countFields(second));
  }

  @Test
  public void collectionDataIsSortedByParentRelationship() {
    CollectionData collectionData = parseDocument("DocPartLevelSorted1.json",
        "DocPartLevelSorted2.json");

    Set<TableRef> parsed = new HashSet<>();
    for (DocPartData docPartData : collectionData.orderedDocPartData()) {
      TableRef tableRef = docPartData.getMetaDocPart().getTableRef();
      if (!tableRef.isRoot()) {
        TableRef parent = tableRef.getParent().get();
        assertTrue(parsed.contains(parent));
      }
      parsed.add(tableRef);
    }

  }

  private int countFields(DocPartRow row) {
    Iterator<KvValue<?>> it = row.getFieldValues().iterator();
    int cont = 0;
    while (it.hasNext()) {
      cont++;
      it.next();
    }
    return cont;
  }

  private DocPartData findRootDocPart(CollectionData collectionData) {
    return findDocPart(collectionData, ROOT_DOC_NAME);
  }

  private DocPartData findDocPart(CollectionData collectionData, String path) {
    ArrayList<String> pathList = new ArrayList<>(Arrays.asList(path.split("\\.")));
    Collections.reverse(pathList);
    pathList.add(ROOT_DOC_NAME);
    String name = pathList.get(0);
    for (DocPartData docPartData : collectionData.orderedDocPartData()) {
      MetaDocPart metaDocPart = docPartData.getMetaDocPart();
      if (name.equals(metaDocPart.getTableRef().getName())) {
        if (isSamePath(pathList, metaDocPart.getTableRef())) {
          return docPartData;
        }
      }
    }
    return null;
  }

  private void assertFieldWithValueExists(DocPartData rootDocPart, DocPartRow firstRow,
      String fieldName, FieldType fieldType, Object fieldValue) {
    int fieldOrder = findFieldPosition(rootDocPart, fieldName, fieldType);
    assertTrue(fieldOrder >= 0);
    assertExistFieldValueInPosition(firstRow, fieldOrder, fieldValue);
  }

  private void assertScalarWithValueExists(DocPartData rootDocPart, DocPartRow firstRow,
      FieldType fieldType, Object fieldValue) {
    int scalarOrder = findScalarPosition(rootDocPart, fieldType);
    assertTrue(scalarOrder >= 0);
    assertExistScalarValueInPosition(firstRow, scalarOrder, fieldValue);
  }

  private boolean isSamePath(ArrayList<String> pathList, TableRef tableRef) {
    int idx = 0;
    Optional<TableRef> table = Optional.of(tableRef);
    while (table.isPresent()) {
      if (!pathList.get(idx).equals(table.get().getName())) {
        return false;
      }
      idx++;
      table = table.get().getParent();
    }
    return true;
  }

  private DocPartRow findRowSeq(DocPartData docPartData, Integer seq) {
    for (DocPartRow row : docPartData) {
      if (row.getSeq() == seq) {
        return row;
      }
    }
    return null;
  }

  private DocPartRow findRowSeq(DocPartData docPartData, int parentId, Integer seq) {
    for (DocPartRow row : docPartData) {
      if (row.getPid() == parentId && row.getSeq() == seq) {
        return row;
      }
    }
    return null;
  }

  private int findFieldPosition(DocPartData docPartData, String name, FieldType type) {
    int idx = 0;
    Iterator<? extends MetaField> iterator = docPartData.orderedMetaFieldIterator();
    while (iterator.hasNext()) {
      MetaField field = iterator.next();
      if (field.getName().equals(name) && field.getType() == type) {
        return idx;
      }
      idx++;
    }
    return -1;
  }

  private int findScalarPosition(DocPartData docPartData, FieldType type) {
    int idx = 0;
    Iterator<? extends MetaScalar> iterator = docPartData.orderedMetaScalarIterator();
    while (iterator.hasNext()) {
      MetaScalar scalar = iterator.next();
      if (scalar.getType() == type) {
        return idx;
      }
      idx++;
    }
    return -1;
  }

  private boolean assertExistFieldValueInPosition(DocPartRow row, int order, Object value) {
    return assertExistValueInPosition(row.getFieldValues(), order, value);
  }

  private boolean assertExistScalarValueInPosition(DocPartRow row, int order, Object value) {
    return assertExistValueInPosition(row.getScalarValues(), order, value);
  }

  private boolean assertExistValueInPosition(Iterable<KvValue<?>> values, int order, Object value) {
    Iterator<KvValue<?>> iterator = values.iterator();
    KvValue<?> kv = null;
    for (int i = 0; i <= order; i++) {
      kv = iterator.next();
    }
    if (kv.getType() == NullType.INSTANCE) {
      assertEquals(value, null);
    } else if (kv.getType() == MongoObjectIdType.INSTANCE) {
      assertArrayEquals((byte[]) value, ((KvMongoObjectId) kv).getArrayValue());
    } else {
      assertEquals(value, kv.getValue());
    }
    return true;
  }

  private CollectionData parseDocument(String... docNames) {
    MockRidGenerator ridGenerator = new MockRidGenerator();
    IdentifierFactory identifierFactory =
        new DefaultIdentifierFactory(new MockIdentifierInterface());
    MutableMetaDatabase db = mutableSnapshot.getMetaDatabaseByName(DB1);
    D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory,
        ridGenerator, db, db.getMetaCollectionByName(COLLA));
    for (String doc : docNames) {
      KvDocument document = parser.createFromResource("docs/" + doc);
      translator.translate(document);
    }
    return translator.getCollectionDataAccumulator();
  }

}
