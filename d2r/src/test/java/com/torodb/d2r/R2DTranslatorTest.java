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
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;

public class R2DTranslatorTest {

	private TableRefFactory fact = new TableRefFactoryImpl();
	private TableRef rootRef = fact.createRoot();
	
	private static final boolean IsArray = true;
	private static final boolean IsDocument = false;
	
	/*
	  Document:
  		{ 
  			"name" : "jero"
  		}
	 */
	@Test
	public void readSimpleDocument(){
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name", "name_s", FieldType.STRING);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		builder.addRow(1, null, 1, null, "jero");
		MockedResultSet root = builder.getResultSet();
		
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		ImmutableList<DocPartResult<MockedResultSet>> lst = new ImmutableList.Builder<DocPartResult<MockedResultSet>>().add(resultSet1).build();
		DocPartResults<MockedResultSet> docPartResultSets = new DocPartResults<>(lst);
		
        R2DTranslator<MockedResultSet> r2dTranslator = new R2DBackedTranslator<MockedResultSet, InternalFields>(new BackendTranslatorMocked());
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        assertEquals(1, readedDocuments.size());
        KVDocument doc = readedDocuments.iterator().next().getRoot();
        assertEquals("jero",doc.get("name").getValue());
	}
	
	/*
	  Document:
		{ 
			"name" : "jero",
			"address" : null
		}
	 */
	@Test
	public void readDocumentWithNullField(){
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name",    "name_s",    FieldType.STRING);
		builder.addMetaField("address", "address_s", FieldType.STRING);
		builder.addMetaField("address", "address_n", FieldType.NULL);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		builder.addRow(1, null, 1, null, "jero", null, true);
		MockedResultSet root = builder.getResultSet();
		
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
	
	/*
	  Document:
		{ 
			"name" : "jero"		
		}
	 */	
	@Test
	public void readDocumentWithOutPresentFields(){
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name",    "name_s",    FieldType.STRING);
		builder.addMetaField("address", "address_s", FieldType.STRING);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		builder.addRow(1, null, 1, null, "jero", null);
		MockedResultSet root = builder.getResultSet();
		
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
	
	/*
	  Document:
		{ 
			"name" : "jero",
			"address" : "my home",
			"age" : 25
		}
	 */
	@Test
	public void readDocumentWithMultipleFields(){
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name", "name_s", FieldType.STRING);
		builder.addMetaField("address", "address_s", FieldType.STRING);
		builder.addMetaField("age", "age_i", FieldType.INTEGER);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		builder.addRow(1, null, 1, null, "jero","my home",25);
		MockedResultSet root = builder.getResultSet();
		
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
	
	/*
	  Document 1
		{ 
			"name" : "jero"
		}
	  Document 2
		{ 
			"name" : "john"
		}
	 */
	@Test
	public void readMultipleDocument(){
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name", "name_s", FieldType.STRING);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		builder.addRow(1, null, 1, null, "jero");
		builder.addRow(2, null, 2, null, "john");
		MockedResultSet root = builder.getResultSet();
		
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

	/*
	  Document:
		{ 
			"name" : "jero",
			"address" : {
				"street" : "myhouse"
			}
		}
	 */
	@Test
	public void readTwoLevelDocument(){
		/* Root Level */
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name",    "name_s",    FieldType.STRING);
		builder.addMetaField("address", "address_e", FieldType.CHILD);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		builder.addRow(1, null, 1, null, "jero",IsDocument);
		MockedResultSet root = builder.getResultSet();
		
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		
		/* Second Level */
		TableRef secondRef = fact.createChild(rootRef, "address");
		MetaDocPartBuilder secondBuilder=new MetaDocPartBuilder(secondRef);
		secondBuilder.addMetaField("street", "street_s", FieldType.STRING);
		MetaDocPart secondLevelDocPart = secondBuilder.buildMetaDocPart();
		secondBuilder.addRow(1, 1, 20, null, "myhouse");
		MockedResultSet secondLevel = secondBuilder.getResultSet();
		
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

	/*
	  Document:
		{ 
			"name" : "jero",
			"numbers" : [4, 8, 15, 16]
		}
	 */
	@Test
	public void readInnerArray(){
		/* Root Level */
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name",    "name_s",    FieldType.STRING);
		builder.addMetaField("numbers", "numbers_e", FieldType.CHILD);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		builder.addRow(1, null, 1, null, "jero",IsArray);
		MockedResultSet root = builder.getResultSet();
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		
		/* Second Level */
		TableRef secondRef = fact.createChild(rootRef, "numbers");
		MetaDocPartBuilder secondBuilder=new MetaDocPartBuilder(secondRef);
		secondBuilder.addMetaScalar("v_i", FieldType.INTEGER);
		MetaDocPart secondLevelDocPart = secondBuilder.buildMetaDocPart();
		secondBuilder.addRow(1, 1, 20, 0,  4);
		secondBuilder.addRow(1, 1, 21, 1,  8);
		secondBuilder.addRow(1, 1, 23, 2, 15);
		secondBuilder.addRow(1, 1, 24, 3, 16);
		MockedResultSet secondLevel = secondBuilder.getResultSet();

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
	
	/*
	  Document:
		{ 
			"name" : "jero",
			"numbers" : [666, [4,8, 15, 16]]
		}
	 */
	@Test
	public void readTwoInnerArray(){
		int did = 1;
		/* Root Level */
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name",    "name_s",    FieldType.STRING);
		builder.addMetaField("numbers", "numbers_e", FieldType.CHILD);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		
		builder.addRow(did, null, did, null, "jero",IsArray);
		MockedResultSet root = builder.getResultSet();
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		
		/* Second Level */
		int pid = did;
		TableRef secondRef = fact.createChild(rootRef, "numbers");
		MetaDocPartBuilder secondBuilder = new MetaDocPartBuilder(secondRef);
		secondBuilder.addMetaScalar("v_i", FieldType.INTEGER);
		secondBuilder.addMetaScalar("v_e", FieldType.CHILD);
		MetaDocPart secondLevelDocPart = secondBuilder.buildMetaDocPart();
		
		secondBuilder.addRow(did, pid, 20, 0,  666,   null);
		secondBuilder.addRow(did, pid, 21, 1, null,IsArray);
		MockedResultSet secondLevel = secondBuilder.getResultSet();
		DocPartResult<MockedResultSet> resultSet2 = new DocPartResult<MockedResultSet>(secondLevelDocPart, secondLevel);
		
		/* Third Level */
		int pid1 = 21;
		TableRef thirdRef = fact.createChild(secondRef, 2);
		MetaDocPartBuilder thirdBuilder = new MetaDocPartBuilder(thirdRef);
		thirdBuilder.addMetaScalar("v_i", FieldType.INTEGER);
		MetaDocPart thirdLevelDocPart = thirdBuilder.buildMetaDocPart();
		
		thirdBuilder.addRow(did, pid1, 30, 0,  4);
		thirdBuilder.addRow(did, pid1, 31, 1,  8);
		thirdBuilder.addRow(did, pid1, 33, 2, 15);
		thirdBuilder.addRow(did, pid1, 34, 3, 16);
		MockedResultSet thirdLevel = thirdBuilder.getResultSet();
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

	/*
	  Document:
		{ 
			"name" : "jero",
			"numbers" : [ 
				666,
				{
					"address": "myhome"
				} 
			]
		}
	 */
	@Test
	public void readDocumentInArray(){
		int did = 1;
		/* Root Level */
		MetaDocPartBuilder builder=new MetaDocPartBuilder(rootRef);
		builder.addMetaField("name",    "name_s",    FieldType.STRING);
		builder.addMetaField("numbers", "numbers_e", FieldType.CHILD);
		MetaDocPart rootDocPart = builder.buildMetaDocPart();
		
		builder.addRow(did, null, did, null, "jero",IsArray);
		MockedResultSet root = builder.getResultSet();
		DocPartResult<MockedResultSet> resultSet1 = new DocPartResult<MockedResultSet>(rootDocPart, root);
		
		/* Second Level */
		int pid = did;
		TableRef secondRef = fact.createChild(rootRef, "numbers");
		MetaDocPartBuilder secondBuilder = new MetaDocPartBuilder(secondRef);
		secondBuilder.addMetaScalar("v_i", FieldType.INTEGER);
		secondBuilder.addMetaField("address", "address_s", FieldType.STRING);
		MetaDocPart secondLevelDocPart = secondBuilder.buildMetaDocPart();
		
		secondBuilder.addRow(did, pid, 20, 0,  666,     null);
		secondBuilder.addRow(did, pid, 21, 1, null, "myhome");
		MockedResultSet secondLevel = secondBuilder.getResultSet();
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
        assertEquals(2,array.size());
        assertEquals(666,array.get(0).getValue());
        KVValue<?> kvValueSecond = array.get(1);
        assertNotNull(kvValueSecond);
        assertTrue(kvValueSecond instanceof KVDocument);
        KVDocument doc2 = (KVDocument) kvValueSecond;
        assertEquals("myhome",doc2.get("address").getValue());
	}
}
