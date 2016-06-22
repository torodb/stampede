package com.torodb.d2r;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.InternalFields;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.d2r.MockedResultSet.MockedRow;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;

public class R2DTranslatorTest {

	private TableRefFactory fact = new TableRefFactoryImpl();
	private TableRef rootRef = fact.createRoot();
	
	private static final boolean IsArray = true;
	private static final boolean IsDocument = false;
	
	
	@Test
	public void readSimpleDocument(){
		ImmutableMetaDocPart.Builder rootBuilder = new ImmutableMetaDocPart.Builder(rootRef, rootRef.getName());
		rootBuilder.add(new ImmutableMetaField("name", "name_s", FieldType.STRING));
		MetaDocPart rootDocPart = rootBuilder.build();
		
		MockedResultSet root = new MockedResultSet(
				new MockedRow(1, null, 1, null, "jero")
		); 
		
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>().add(resultSet1).build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        assertEquals(1, readedDocuments.size());
        KVDocument doc = readedDocuments.iterator().next().getRoot();
        assertEquals("jero",doc.get("name").getValue());
	}
	
	@Test
	public void readDocumentWithNullField(){
		ImmutableMetaDocPart.Builder rootBuilder = new ImmutableMetaDocPart.Builder(rootRef, rootRef.getName());
		rootBuilder.add(new ImmutableMetaField("name",    "name_s",    FieldType.STRING));
		rootBuilder.add(new ImmutableMetaField("address", "address_s", FieldType.STRING));
		MetaDocPart rootDocPart = rootBuilder.build();
		
		MockedResultSet root = new MockedResultSet(
				new MockedRow(1, null, 1, null, "jero", KVNull.getInstance())
		); 
		
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>().add(resultSet1).build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        assertEquals(1, readedDocuments.size());
        KVDocument doc = readedDocuments.iterator().next().getRoot();
        assertEquals("jero",doc.get("name").getValue());
        assertEquals(KVNull.getInstance(),doc.get("address"));
	}
	
	@Test
	public void readDocumentWithOutPresentFields(){
		ImmutableMetaDocPart.Builder rootBuilder = new ImmutableMetaDocPart.Builder(rootRef, rootRef.getName());
		rootBuilder.add(new ImmutableMetaField("name",    "name_s",    FieldType.STRING));
		rootBuilder.add(new ImmutableMetaField("address", "address_s", FieldType.STRING));
		MetaDocPart rootDocPart = rootBuilder.build();
		
		MockedResultSet root = new MockedResultSet(
				new MockedRow(1, null, 1, null, "jero", null)
		); 
		
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>().add(resultSet1).build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        assertEquals(1, readedDocuments.size());
        KVDocument doc = readedDocuments.iterator().next().getRoot();
        assertEquals("jero", doc.get("name").getValue());
        assertNull(doc.get("address"));
	}
	
	
	@Test
	public void readDocumentWithMultipleFields(){
		ImmutableMetaDocPart.Builder rootBuilder = new ImmutableMetaDocPart.Builder(rootRef, rootRef.getName());
		rootBuilder.add(new ImmutableMetaField("name", "name_s", FieldType.STRING))
				   .add(new ImmutableMetaField("address", "address_s", FieldType.STRING))
				   .add(new ImmutableMetaField("age", "age_i", FieldType.INTEGER));
		MetaDocPart rootDocPart = rootBuilder.build();
		
		MockedResultSet root = new MockedResultSet(
				new MockedRow(1, null, 1, null, "jero","my home",25)
		); 
		
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>().add(resultSet1).build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        assertEquals(1, readedDocuments.size());
        KVDocument doc = readedDocuments.iterator().next().getRoot();
        assertEquals("jero",doc.get("name").getValue());
        assertEquals("my home",doc.get("address").getValue());
        assertEquals(25,doc.get("age").getValue());
	}
	
	@Test
	public void readMultipleDocument(){
		ImmutableMetaDocPart.Builder rootBuilder = new ImmutableMetaDocPart.Builder(rootRef, rootRef.getName());
		rootBuilder.add(new ImmutableMetaField("name", "name_s", FieldType.STRING));
		MetaDocPart rootDocPart = rootBuilder.build();
		
		MockedResultSet root = new MockedResultSet(
				new MockedRow(1, null, 1, null, "jero"),
				new MockedRow(2, null, 2, null, "john")
		); 
		
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>().add(resultSet1).build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        ToroDocument[] readedDocuments = r2dTranslator.translate(docPartResultSets).toArray(new ToroDocument[0]);
        assertEquals(2, readedDocuments.length);
        
        KVDocument doc1 = readedDocuments[0].getRoot();
        assertEquals("jero",doc1.get("name").getValue());
        
        KVDocument doc2 = readedDocuments[1].getRoot();
        assertEquals("john",doc2.get("name").getValue());

	}

	@Test
	public void readTwoLevelDocument(){
		/* Root Level */
		ImmutableMetaDocPart.Builder rootBuilder = new ImmutableMetaDocPart.Builder(rootRef, rootRef.getName());
		rootBuilder.add(new ImmutableMetaField("name",    "name_s",    FieldType.STRING));
		rootBuilder.add(new ImmutableMetaField("address", "address_e", FieldType.CHILD));
		MetaDocPart rootDocPart = rootBuilder.build();
		
		MockedResultSet root = new MockedResultSet(
				new MockedRow(1, null, 1, null, "jero",IsDocument)
		); 
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		
		/* Second Level */
		TableRef secondRef = fact.createChild(rootRef, "address");
		ImmutableMetaDocPart.Builder secondBuilder = new ImmutableMetaDocPart.Builder(secondRef, secondRef.getName());
		secondBuilder.add(new ImmutableMetaField("street", "street_s", FieldType.STRING));
		MetaDocPart secondLevelDocPart = secondBuilder.build();
		
		MockedResultSet secondLevel = new MockedResultSet(
				new MockedRow(1, 1, 20, null, "myhouse")
		); 
		DocPartResult<MockedResultSet> resultSet2 = new DocPartResult<MockedResultSet>(secondLevelDocPart, secondLevel);
		
		
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>()
				.add(resultSet2)
				.add(resultSet1)
			.build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        assertEquals(1, readedDocuments.size());
        KVDocument doc = readedDocuments.iterator().next().getRoot();
        assertEquals("jero",doc.get("name").getValue());
        KVValue<?> kvValue = doc.get("address");
        assertTrue(kvValue instanceof KVDocument);
	}

	
	@Test
	public void readInnerArray(){
		/* Root Level */
		ImmutableMetaDocPart.Builder rootBuilder = new ImmutableMetaDocPart.Builder(rootRef, rootRef.getName());
		rootBuilder.add(new ImmutableMetaField("name",    "name_s",    FieldType.STRING));
		rootBuilder.add(new ImmutableMetaField("numbers", "numbers_e", FieldType.CHILD));
		MetaDocPart rootDocPart = rootBuilder.build();
		
		MockedResultSet root = new MockedResultSet(
				new MockedRow(1, null, 1, null, "jero",IsArray)
		); 
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		
		/* Second Level */
		TableRef secondRef = fact.createChild(rootRef, "numbers");
		ImmutableMetaDocPart.Builder secondBuilder = new ImmutableMetaDocPart.Builder(secondRef, secondRef.getName());
		secondBuilder.add(new ImmutableMetaScalar("v_i", FieldType.INTEGER));
		MetaDocPart secondLevelDocPart = secondBuilder.build();
		
		MockedResultSet secondLevel = new MockedResultSet(
				new MockedRow(1, 1, 20, 0,  4),
				new MockedRow(1, 1, 21, 1,  8),
				new MockedRow(1, 1, 23, 2, 15),
				new MockedRow(1, 1, 24, 3, 16)
		); 
		DocPartResult<MockedResultSet> resultSet2 = new DocPartResult<MockedResultSet>(secondLevelDocPart, secondLevel);
		
		
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>()
				.add(resultSet2)
				.add(resultSet1)
			.build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        assertEquals(1, readedDocuments.size());
        KVDocument doc = readedDocuments.iterator().next().getRoot();
        assertEquals("jero",doc.get("name").getValue());
        KVValue<?> kvValue = doc.get("numbers");
        assertTrue(kvValue instanceof KVArray);
        KVArray array = (KVArray)kvValue;
        assertEquals(4,array.size());
        assertEquals(4,array.get(0).getValue());
        assertEquals(8,array.get(1).getValue());
        assertEquals(15,array.get(2).getValue());
        assertEquals(16,array.get(3).getValue());
	}
	
	@Test
	public void readTwoInnerArray(){
		int did = 1;
		/* Root Level */
		ImmutableMetaDocPart.Builder rootBuilder = new ImmutableMetaDocPart.Builder(rootRef, rootRef.getName());
		rootBuilder.add(new ImmutableMetaField("name",    "name_s",    FieldType.STRING));
		rootBuilder.add(new ImmutableMetaField("numbers", "numbers_e", FieldType.CHILD));
		MetaDocPart rootDocPart = rootBuilder.build();
		
		MockedResultSet root = new MockedResultSet(
				new MockedRow(did, null, did, null, "jero",IsArray)
		); 
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		
		/* Second Level */
		int pid = did;
		TableRef secondRef = fact.createChild(rootRef, "numbers");
		ImmutableMetaDocPart.Builder secondBuilder = new ImmutableMetaDocPart.Builder(secondRef, secondRef.getName());
		secondBuilder.add(new ImmutableMetaScalar("v_i", FieldType.INTEGER));
		secondBuilder.add(new ImmutableMetaScalar("v_e", FieldType.CHILD));
		MetaDocPart secondLevelDocPart = secondBuilder.build();
		
		MockedResultSet secondLevel = new MockedResultSet(
				new MockedRow(did, pid, 20, 0,  666,   null),
				new MockedRow(did, pid, 21, 1, null,IsArray)
		); 
		DocPartResult<MockedResultSet> resultSet2 = new DocPartResult<MockedResultSet>(secondLevelDocPart, secondLevel);
		
		/* Third Level */
		int pid1 = 21;
		TableRef thirdRef = fact.createChild(secondRef, 2);
		ImmutableMetaDocPart.Builder thirdBuilder = new ImmutableMetaDocPart.Builder(thirdRef, thirdRef.getName());
		thirdBuilder.add(new ImmutableMetaScalar("v_i", FieldType.INTEGER));
		MetaDocPart thirdLevelDocPart = thirdBuilder.build();
		
		MockedResultSet thirdLevel = new MockedResultSet(
				new MockedRow(did, pid1, 30, 0, 4),
				new MockedRow(did, pid1, 31, 1, 8),
				new MockedRow(did, pid1, 33, 2, 15),
				new MockedRow(did, pid1, 34, 3, 16)
		); 
		DocPartResult<MockedResultSet> resultSet3 = new DocPartResult<MockedResultSet>(thirdLevelDocPart, thirdLevel);
		
		
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>()
				.add(resultSet3)
				.add(resultSet2)
				.add(resultSet1)
			.build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        assertEquals(1, readedDocuments.size());
        KVDocument doc = readedDocuments.iterator().next().getRoot();
        assertEquals("jero",doc.get("name").getValue());
        KVValue<?> kvValue = doc.get("numbers");
        assertTrue(kvValue instanceof KVArray);
        KVArray array = (KVArray)kvValue;
        assertEquals(2,array.size());
        assertEquals(666,array.get(0).getValue());
        KVValue<?> kvValueSecond = array.get(1);
        assertNotNull(kvValueSecond);
        assertTrue(kvValueSecond instanceof KVArray);
        KVArray array2 = (KVArray) kvValueSecond;
        assertEquals(4,array2.size());
        assertEquals(4,array2.get(0).getValue());
        assertEquals(8,array2.get(1).getValue());
        assertEquals(15,array2.get(2).getValue());
        assertEquals(16,array2.get(3).getValue());
        
	}

}
