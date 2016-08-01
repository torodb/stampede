/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.integration.backend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;

import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.TableRefComparator;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.meta.SnapshotUpdater;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockIdentifierInterface;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.d2r.R2DTranslatorImpl;
import com.torodb.kvdocument.conversion.json.JacksonJsonParser;
import com.torodb.kvdocument.conversion.json.JsonParser;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

public abstract class AbstractBackendTest {

    @ClassRule
    public final static BackendRunnerClassRule BACKEND_RUNNER_CLASS_RULE = new BackendRunnerClassRule();
    
    protected static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    
    protected TestData data;
    protected SchemaUpdater schemaUpdater;
    protected SqlInterface sqlInterface;
    protected SqlHelper sqlHelper;
    
    private MockRidGenerator ridGenerator = new MockRidGenerator();
    private IdentifierFactory identifierFactory = new IdentifierFactoryImpl(new MockIdentifierInterface());
    
    @Before
    public void setUp() throws Exception {
        schemaUpdater = BACKEND_RUNNER_CLASS_RULE.getSchemaUpdater();
        sqlInterface = BACKEND_RUNNER_CLASS_RULE.getSqlInterface();
        sqlHelper = BACKEND_RUNNER_CLASS_RULE.getSqlHelper();
        data = new TestData(tableRefFactory, sqlInterface);
        BACKEND_RUNNER_CLASS_RULE.cleanDatabase();
    }

    protected ImmutableMetaSnapshot buildMetaSnapshot() {
        MvccMetainfoRepository metainfoRepository = new MvccMetainfoRepository();
        SnapshotUpdater.updateSnapshot(metainfoRepository, sqlInterface, sqlHelper, schemaUpdater, tableRefFactory);

        try (SnapshotStage stage = metainfoRepository.startSnapshotStage()) {
            return stage.createImmutableSnapshot();
        }
    }
    
    protected void createMetaModel(DSLContext dsl) {
        String databaseName = data.database.getName();
        String databaseSchemaName = data.database.getIdentifier();
        String collectionName = data.collection.getName();
        
        sqlInterface.getMetaDataWriteInterface().addMetaDatabase(dsl, databaseName, databaseSchemaName);
        sqlInterface.getStructureInterface().createSchema(dsl, databaseSchemaName);
        sqlInterface.getMetaDataWriteInterface().addMetaCollection(dsl, databaseName, collectionName, data.collection.getIdentifier());
        sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dsl, databaseName, collectionName, data.rootDocPart.getTableRef(), data.rootDocPart.getIdentifier());
        sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dsl, databaseName, collectionName, data.subDocPart.getTableRef(), data.subDocPart.getIdentifier());
    }
    
    protected void insertMetaFields(DSLContext dsl, MetaDocPart metaDocPart){
        metaDocPart.streamFields().forEach( metaField ->
            sqlInterface.getMetaDataWriteInterface().addMetaField(dsl, data.database.getName(), data.collection.getName(), metaDocPart.getTableRef(), 
                    metaField.getName(), metaField.getIdentifier(), metaField.getType())
        );
    }
    
    protected void insertNewMetaFields(DSLContext dsl, MutableMetaDocPart metaDocPart){
        for (MetaField metaField : metaDocPart.getAddedMetaFields()) {
            sqlInterface.getMetaDataWriteInterface().addMetaField(dsl, data.database.getName(), data.collection.getName(), metaDocPart.getTableRef(), 
                    metaField.getName(), metaField.getIdentifier(), metaField.getType());
        }
    }
    
    protected void createDocPartTable(DSLContext dsl, MetaCollection metaCollection, MetaDocPart metaDocPart) {
        if (metaDocPart.getTableRef().isRoot()) {
            sqlInterface.getStructureInterface().createRootDocPartTable(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef());
            sqlInterface.getStructureInterface().addRootDocPartTableIndexes(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef());
        } else {
            sqlInterface.getStructureInterface().createDocPartTable(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef(),
                    metaCollection.getMetaDocPartByTableRef(metaDocPart.getTableRef().getParent().get()).getIdentifier());
            sqlInterface.getStructureInterface().addDocPartTableIndexes(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef(),
                    metaCollection.getMetaDocPartByTableRef(metaDocPart.getTableRef().getParent().get()).getIdentifier());
        }
        
        addColumnsToDocPartTable(dsl, metaCollection, metaDocPart);
    }
    
    protected void addColumnsToDocPartTable(DSLContext dsl, MetaCollection metaCollection, MetaDocPart metaDocPart) {
        metaDocPart.streamScalars().forEach(metaScalar -> 
            sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), 
                    metaScalar.getIdentifier(), (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaScalar.getType()))
        );
        
        metaDocPart.streamFields().forEach(metaField -> 
            sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), 
                    metaField.getIdentifier(), (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaField.getType()))
        );
    }
    
    protected void addNewColumnsToDocPartTable(DSLContext dsl, MetaCollection metaCollection, MutableMetaDocPart mutableMetaDocPart) {
        for (MetaScalar metaScalar : mutableMetaDocPart.getAddedMetaScalars()) { 
            sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, data.database.getIdentifier(), mutableMetaDocPart.getIdentifier(), 
                    metaScalar.getIdentifier(), (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaScalar.getType()));
        }
        
        for (MetaField metaField : mutableMetaDocPart.getAddedMetaFields()) { 
            sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, data.database.getIdentifier(), mutableMetaDocPart.getIdentifier(), 
                    metaField.getIdentifier(), (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaField.getType()));
        }
    }

    protected Collection<ToroDocument> readDocuments(MetaDatabase metaDatabase, MetaCollection metaCollection,
            Iterator<DocPartResult> docPartResultSets) {
        R2DTranslator r2dTranslator = new R2DTranslatorImpl();
        Collection<ToroDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        return readedDocuments;
    }

    protected List<Integer> writeCollectionData(DSLContext dsl, CollectionData collectionData) {
        return writeCollectionData(dsl, data.database.getIdentifier(), collectionData);
    }

    protected List<Integer> writeCollectionData(DSLContext dsl, String databaseIdentifier, CollectionData collectionData) {
        Iterator<DocPartData> docPartDataIterator = StreamSupport.stream(collectionData.orderedDocPartData().spliterator(), false)
                .iterator();
        List<Integer> generatedDids = new ArrayList<>();
        while (docPartDataIterator.hasNext()) {
            DocPartData docPartData = docPartDataIterator.next();
            if (docPartData.getMetaDocPart().getTableRef().isRoot()) {
                docPartData.forEach(docPartRow ->generatedDids.add(docPartRow.getDid()));
            }
            sqlInterface.getWriteInterface().insertDocPartData(dsl, databaseIdentifier, docPartData);
        }
        return generatedDids;
    }

    protected CollectionData parseDocumentAndCreateDocPartDataTables(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, KVDocument document)
            throws Exception {
        return parseDocumentsAndCreateDocPartDataTables(mutableSnapshot, dsl, Arrays.asList(document));
    }

    protected CollectionData parseDocumentsAndCreateDocPartDataTables(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, List<KVDocument> documents)
            throws Exception {
        CollectionData collectionData = parseDocuments(mutableSnapshot, dsl, documents);
        
        mutableSnapshot.streamMetaDatabases().forEachOrdered(metaDatabase -> {
            metaDatabase.streamMetaCollections().forEachOrdered(metaCollection -> {
                metaCollection.streamContainedMetaDocParts().sorted(TableRefComparator.MetaDocPart.ASC).forEachOrdered(metaDocPart -> {
                    if (metaDocPart.getTableRef().isRoot()) {
                        sqlInterface.getStructureInterface().createRootDocPartTable(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef());
                        sqlInterface.getStructureInterface().addRootDocPartTableIndexes(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef());
                    } else {
                        sqlInterface.getStructureInterface().createDocPartTable(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef(),
                                metaCollection.getMetaDocPartByTableRef(metaDocPart.getTableRef().getParent().get()).getIdentifier());
                        sqlInterface.getStructureInterface().addDocPartTableIndexes(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef(),
                                metaCollection.getMetaDocPartByTableRef(metaDocPart.getTableRef().getParent().get()).getIdentifier());
                    }
                    metaDocPart.streamScalars().forEachOrdered(metaScalar -> {
                        sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), 
                                metaScalar.getIdentifier(), sqlInterface.getDataTypeProvider().getDataType(metaScalar.getType()));
                    });
                    metaDocPart.streamFields().forEachOrdered(metaField -> {
                        sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, data.database.getIdentifier(), metaDocPart.getIdentifier(), 
                                metaField.getIdentifier(), sqlInterface.getDataTypeProvider().getDataType(metaField.getType()));
                    });
                });
            });
        });
        
        return collectionData;
    }

    protected CollectionData parseDocuments(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, List<KVDocument> documents)
            throws Exception {
        return parseDocuments(mutableSnapshot, data.database.getName(), data.collection.getName(), dsl, documents);
    }

    protected CollectionData parseDocuments(MutableMetaSnapshot mutableSnapshot, String databaseName, String collectionName, DSLContext dsl, List<KVDocument> documents)
            throws Exception {
        CollectionData collectionData = readDataFromDocuments(databaseName, collectionName, documents, mutableSnapshot);
        
        return collectionData;
    }
    
    protected KVDocument parseFromJson(String jsonFileName) throws Exception {
        JsonParser parser = new JacksonJsonParser();
        return parser.createFromResource("docs/" + jsonFileName);
    }
    
    protected List<KVDocument> parseListFromJson(String jsonFileName) throws Exception {
        JsonParser parser = new JacksonJsonParser();
        return parser.createListFromResource("docs/" + jsonFileName);
    }
    
    protected CollectionData readDataFromDocuments(String database, String collection, List<KVDocument> documents, MutableMetaSnapshot mutableSnapshot) throws Exception {
        MutableMetaDatabase db = mutableSnapshot.getMetaDatabaseByName(database);
        D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, db, db.getMetaCollectionByName(collection));
        documents.forEach(translator::translate);
        return translator.getCollectionDataAccumulator();
    }

    protected TableRef createTableRef(String...names) {
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
