package com.torodb.d2r;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.torodb.common.util.HexUtils;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.conversion.json.JacksonJsonParser;
import com.torodb.kvdocument.conversion.json.JsonParser;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVMongoObjectId;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

public class Document2RelStackTest {
	
    private final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    
	//TODO: Change to final implementation code
	private static final String ARRAY_VALUE_NAME = "v";
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
			.add(new ImmutableMetaDatabase.Builder(DB1, DB1)
					.add(new ImmutableMetaCollection.Builder(COLLA, COLLA).build())
					.add(new ImmutableMetaCollection.Builder(COLLB, COLLB).build()).build())
			.add(new ImmutableMetaDatabase.Builder(DB2, DB2)
					.add(new ImmutableMetaCollection.Builder(COLLC, COLLC).build())
					.add(new ImmutableMetaCollection.Builder(COLLD, COLLD).build()).build())
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
		assertFalse(firstRow.iterator().hasNext());
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
		assertTrue(fieldPosition>=0);
		assertTrue(rootDocPart.iterator().hasNext());
		assertExistValueInPosition(firstRow, 0, "John");
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
		
		assertFieldWithValueExists(monthsDocPart, firstRow, ARRAY_VALUE_NAME, FieldType.INTEGER, 1);
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
		
		DocPartData departmentDocPart = findDocPart(collectionData,"department");
		DocPartRow firstRow = departmentDocPart.iterator().next();
		assertNotNull(firstRow);
		assertFalse(firstRow.iterator().hasNext());
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
		assertFieldWithValueExists(departmentsDocPart, row4Document, "dept", FieldType.CHILD, IS_SUBDOCUMENT);

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
		assertFieldWithValueExists(monthsDocPart, firstRowMonths, ARRAY_VALUE_NAME, FieldType.CHILD, IS_ARRAY);

		DocPartData subArrayDocPart = findDocPart(collectionData, "months.$2");
		assertNotNull(subArrayDocPart);

		DocPartRow firstRowSubArray = findRowSeq(subArrayDocPart, firstRow.getRid(), 0);
		assertNotNull(firstRowSubArray);
		int fieldSubArray = findFieldPosition(subArrayDocPart, ARRAY_VALUE_NAME, FieldType.INTEGER);
		assertTrue(fieldSubArray >= 0);
		assertExistValueInPosition(firstRowSubArray, fieldSubArray, 1);
	}
	
	@Test
	public void emptyArrayInArrayMapsToATable() {
		CollectionData collectionData = parseDocument("MultiArrayEmpty.json");

		DocPartData monthsDocPart = findDocPart(collectionData, "months");
		DocPartRow firstRow = monthsDocPart.iterator().next();

		assertFieldWithValueExists(monthsDocPart, firstRow, ARRAY_VALUE_NAME, FieldType.CHILD, IS_ARRAY);

		DocPartData subArrayDocPart = findDocPart(collectionData, "months.$2");
		assertNotNull(subArrayDocPart);
	}
	
	@Test
	public void mapFieldTypes() {
		CollectionData collectionData = parseDocument("FieldTypes.json");
		DocPartData rootDocPart = findRootDocPart(collectionData);
		DocPartRow firstRow = rootDocPart.iterator().next();
		
		assertFieldWithValueExists(rootDocPart, firstRow, "_id", FieldType.MONGO_OBJECT_ID, HexUtils.hex2Bytes("5298a5a03b3f4220588fe57c"));
		assertFieldWithValueExists(rootDocPart, firstRow, "null", FieldType.NULL, null);
		assertFieldWithValueExists(rootDocPart, firstRow, "boolean", FieldType.BOOLEAN, true);
		assertFieldWithValueExists(rootDocPart, firstRow, "integer", FieldType.INTEGER, 10);
		assertFieldWithValueExists(rootDocPart, firstRow, "double", FieldType.DOUBLE, 10.2);
		assertFieldWithValueExists(rootDocPart, firstRow, "string", FieldType.STRING, "john");
		assertFieldWithValueExists(rootDocPart, firstRow, "long", FieldType.LONG, 10020202020L);
		DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
		assertFieldWithValueExists(rootDocPart, firstRow, "date", FieldType.INSTANT, Instant.from(sdf.parse("2015-06-18T16:43:58.967Z")));
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
		CollectionData collectionData = parseDocument("DocPartLevelSorted1.json","DocPartLevelSorted2.json");

		Set<TableRef> parsed = new HashSet<>();		
		for (DocPartData docPartData : collectionData) {
			TableRef tableRef = docPartData.getMetaDocPart().getTableRef();
			if (!tableRef.isRoot()){
				TableRef parent = tableRef.getParent().get();
				assertTrue(parsed.contains(parent));
			}
			parsed.add(tableRef);
		}
		
	}
	
	private int countFields(DocPartRow row) {
		Iterator<KVValue<?>> it = row.iterator();
		int cont = 0;
		while (it.hasNext()) {
			cont++;
			it.next();
		}
		return cont;
	}
	
	
	private DocPartData findRootDocPart(CollectionData collectionData){
		return findDocPart(collectionData,ROOT_DOC_NAME);
	}
	
	private DocPartData findDocPart(CollectionData collectionData, String path){
		ArrayList<String> pathList=new ArrayList<>(Arrays.asList(path.split("\\.")));
		Collections.reverse(pathList);
		pathList.add(ROOT_DOC_NAME);
		String name = pathList.get(0); 
		for(DocPartData docPartData :collectionData){
			MetaDocPart metaDocPart = docPartData.getMetaDocPart();
			if (name.equals(metaDocPart.getTableRef().getName())){
				if (isSamePath(pathList, metaDocPart.getTableRef())){
					return docPartData;
				}
			}
		}
		return null;
	}

	private void assertFieldWithValueExists(DocPartData rootDocPart, DocPartRow firstRow, String fieldName, FieldType fieldType, Object fieldValue) {
		int fieldOrder = findFieldPosition(rootDocPart, fieldName, fieldType);
		assertTrue(fieldOrder>=0);
		assertExistValueInPosition(firstRow, fieldOrder, fieldValue);
	}

	private boolean isSamePath(ArrayList<String> pathList, TableRef tableRef){
		int idx=0;
		Optional<TableRef> table = Optional.of(tableRef);
		while (table.isPresent()){
			if (!pathList.get(idx).equals(table.get().getName())){
				return false;
			}
			idx++;
			table=table.get().getParent();
		}
		return true;
	}
	
	private DocPartRow findRowSeq(DocPartData docPartData, Integer seq){
		for(DocPartRow row: docPartData){
			if (row.getSeq()==seq){
				return row;
			}
		}
		return null;
	}
	
	private DocPartRow findRowSeq(DocPartData docPartData, int parentId, Integer seq){
		for(DocPartRow row: docPartData){
			if (row.getPid()==parentId && row.getSeq()==seq){
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
			if (field.getName().equals(name) && field.getType()==type){
				return idx; 
			}
			idx++;
		}
		return -1;
	}
	
	private boolean assertExistValueInPosition(DocPartRow row,int order, Object value){
		KVValue<?> kv = null;
		Iterator<KVValue<?>> iterator = row.iterator();
		for (int i=0;i<=order;i++){
			kv = iterator.next();
		}
		if (kv.getType()==NullType.INSTANCE){
			assertEquals(value,null);
		}else if (kv.getType()==MongoObjectIdType.INSTANCE){
			assertArrayEquals((byte[]) value,((KVMongoObjectId) kv).getArrayValue());
		}else{
			assertEquals(value,kv.getValue());
		}
		return true;
	}

	private CollectionData parseDocument(String ...docNames) {
		MockRidGenerator ridGenerator = new MockRidGenerator();
		IdentifierFactory identifierFactory = new IdentifierFactoryImpl();
		D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, mutableSnapshot, DB1, COLLA);
		for (String doc: docNames){
			KVDocument document = parser.createFromResource("docs/"+doc);
			translator.translate(document);
		}
		return translator.getCollectionDataAccumulator();
	}

}