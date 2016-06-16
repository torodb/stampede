package com.torodb.backend;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;

import com.torodb.backend.d2r.R2DBackendTranslatorImpl;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockIdentifierInterface;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.d2r.R2DBackedTranslator;
import com.torodb.kvdocument.conversion.json.JacksonJsonParser;
import com.torodb.kvdocument.conversion.json.JsonParser;
import com.torodb.kvdocument.values.KVDocument;

public class BackendDocumentTestHelper {
	
	private DatabaseInterface databaseInterface;
	private TableRefFactory tableRefFactory;
	private TestSchema schema;
	
	private MockRidGenerator ridGenerator = new MockRidGenerator();
	private IdentifierFactory identifierFactory = new IdentifierFactoryImpl(new MockIdentifierInterface());
	
	public BackendDocumentTestHelper(DatabaseInterface databaseInterface, TableRefFactory tableRefFactory, TestSchema schema){
		this.databaseInterface = databaseInterface;
		this.tableRefFactory = tableRefFactory;
		this.schema = schema;
	}

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Collection<ToroDocument> readDocuments(MetaDatabase metaDatabase, MetaCollection metaCollection,
            DocPartResults<ResultSet> docPartResultSets) {
        R2DTranslator r2dTranslator = new R2DBackedTranslator(new R2DBackendTranslatorImpl(databaseInterface, metaDatabase, metaCollection));
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
            databaseInterface.insertDocPartData(dsl, schema.databaseSchemaName, docPartData);
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
                    List<Field<?>> fields = new ArrayList<>(databaseInterface.getDocPartTableInternalFields(metaDocPart));
                    metaDocPart.streamFields().forEachOrdered(metaField -> {
                        fields.add(DSL.field(metaField.getIdentifier(), databaseInterface.getDataType(metaField.getType())));
                    });
                    dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), schema.databaseSchemaName, metaDocPart.getIdentifier(), fields));
                });
            });
        });
        return collectionData;
    }
    
    public KVDocument parseFromJson(String jsonFileName) throws Exception {
    	JsonParser parser = new JacksonJsonParser();
        return parser.createFromResource("docs/" + jsonFileName);
    }
    
    public CollectionData readDataFromDocuments(String database, String collection, List<KVDocument> documents, MutableMetaSnapshot mutableSnapshot) throws Exception {
        D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, mutableSnapshot, database, collection);
        documents.forEach(translator::translate);
        return translator.getCollectionDataAccumulator();
    }
    
}
