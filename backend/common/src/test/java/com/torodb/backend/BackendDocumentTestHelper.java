package com.torodb.backend;

import com.torodb.backend.d2r.R2DBackendTranslatorImpl;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.*;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.*;
import com.torodb.kvdocument.conversion.json.JacksonJsonParser;
import com.torodb.kvdocument.conversion.json.JsonParser;
import com.torodb.kvdocument.values.KVDocument;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.StreamSupport;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class BackendDocumentTestHelper {
	
	private SqlInterface sqlInterface;
	private TableRefFactory tableRefFactory;
	private TestSchema schema;
	
	private MockRidGenerator ridGenerator = new MockRidGenerator();
	private IdentifierFactory identifierFactory = new IdentifierFactoryImpl(new MockIdentifierInterface());
	
	public BackendDocumentTestHelper(SqlInterface sqlInterface, TableRefFactory tableRefFactory, TestSchema schema){
		this.sqlInterface = sqlInterface;
		this.tableRefFactory = tableRefFactory;
		this.schema = schema;
	}

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Collection<ToroDocument> readDocuments(MetaDatabase metaDatabase, MetaCollection metaCollection,
            DocPartResults<ResultSet> docPartResultSets) {
        R2DTranslator r2dTranslator = new R2DBackedTranslator(new R2DBackendTranslatorImpl(sqlInterface, metaDatabase, metaCollection));
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        return readedDocuments;
    }

    public List<Integer> writeCollectionData(DSLContext dsl, CollectionData collectionData) {
        Iterator<DocPartData> docPartDataIterator = StreamSupport.stream(collectionData.spliterator(), false)
                .iterator();
        List<Integer> generatedDids = new ArrayList<>();
        while (docPartDataIterator.hasNext()) {
            DocPartData docPartData = docPartDataIterator.next();
            if (docPartData.getMetaDocPart().getTableRef().isRoot()) {
                docPartData.forEach(docPartRow ->generatedDids.add(docPartRow.getDid()));
            }
            sqlInterface.insertDocPartData(dsl, schema.databaseSchemaName, docPartData);
        }
        return generatedDids;
    }

    public CollectionData parseDocumentAndCreateDocPartDataTables(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, KVDocument document)
            throws Exception {
    	return parseDocumentsAndCreateDocPartDataTables(mutableSnapshot, dsl, Arrays.asList(document));
    }

    public CollectionData parseDocumentsAndCreateDocPartDataTables(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, List<KVDocument> documents)
            throws Exception {
        CollectionData collectionData = readDataFromDocuments(schema.databaseName, schema.collectionName, documents, mutableSnapshot);
        mutableSnapshot.streamMetaDatabases().forEachOrdered(metaDatabase -> {
            metaDatabase.streamMetaCollections().forEachOrdered(metaCollection -> {
                metaCollection.streamContainedMetaDocParts().sorted(TableRefComparator.MetaDocPart.ASC).forEachOrdered(metaDocPart -> {
                    List<Field<?>> fields = new ArrayList<>(sqlInterface.getDocPartTableInternalFields(metaDocPart));
                    metaDocPart.streamFields().forEachOrdered(metaField -> {
                        fields.add(DSL.field(metaField.getIdentifier(), sqlInterface.getDataType(metaField.getType())));
                    });
                    metaDocPart.streamScalars().forEachOrdered(metaScalar -> {
                        fields.add(DSL.field(metaScalar.getIdentifier(), sqlInterface.getDataType(metaScalar.getType())));
                    });
                    sqlInterface.createDocPartTable(dsl, schema.databaseSchemaName, metaDocPart.getIdentifier(), fields);
                });
            });
        });
        return collectionData;
    }
    
    public KVDocument parseFromJson(String jsonFileName) throws Exception {
        JsonParser parser = new JacksonJsonParser();
        return parser.createFromResource("docs/" + jsonFileName);
    }
    
    public List<KVDocument> parseListFromJson(String jsonFileName) throws Exception {
        JsonParser parser = new JacksonJsonParser();
        return parser.createListFromResource("docs/" + jsonFileName);
    }
    
    public CollectionData readDataFromDocuments(String database, String collection, List<KVDocument> documents, MutableMetaSnapshot mutableSnapshot) throws Exception {
        MutableMetaDatabase db = mutableSnapshot.getMetaDatabaseByName(database);
        D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, db, db.getMetaCollectionByName(collection));
        documents.forEach(translator::translate);
        return translator.getCollectionDataAccumulator();
    }
    
    public TableRef createTableRef(String...names) {
        TableRef tableRef = tableRefFactory.createRoot();
        
        for (String name : names) {
            try {
                int index = Integer.parseInt(name);
                tableRef = tableRefFactory.createChild(tableRef, index);
            } catch(NumberFormatException ex) {
                tableRef = tableRefFactory.createChild(tableRef, name);
            }
        }
        
        return tableRef;
    }

    
}
