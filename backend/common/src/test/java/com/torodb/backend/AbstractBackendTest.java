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

package com.torodb.backend;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.Before;

import com.torodb.backend.d2r.R2DBackendTranslatorImpl;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.BackendException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.d2r.R2DBackedTranslator;
import com.torodb.kvdocument.conversion.json.JacksonJsonParser;
import com.torodb.kvdocument.conversion.json.JsonParser;
import com.torodb.kvdocument.values.KVDocument;

public abstract class AbstractBackendTest {
    
    protected static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    
    protected TestSchema schema;
    protected DatabaseInterface databaseInterface;

    protected JsonParser parser = new JacksonJsonParser();
    
    protected DataSource dataSource;
    
    @Before
    public void setUp() throws Exception {
        databaseInterface = createDatabaseInterface();
        schema = new TestSchema(tableRefFactory, databaseInterface);

        dataSource = createDataSource();
        cleanDatabase(databaseInterface, dataSource);
    }

    protected abstract DataSource createDataSource();

    protected abstract DatabaseInterface createDatabaseInterface();
    
    protected abstract void cleanDatabase(DatabaseInterface databaseInterface, DataSource dataSource) throws SQLException;
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Collection<KVDocument> readDocuments(MetaDatabase metaDatabase, MetaCollection metaCollection,
            DocPartResults<ResultSet> docPartResultSets) throws BackendException, RollbackException {
        R2DTranslator r2dTranslator = new R2DBackedTranslator(new R2DBackendTranslatorImpl(databaseInterface, metaDatabase, metaCollection));
        Collection<KVDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        return readedDocuments;
    }

    protected List<Integer> writeCollectionData(DSLContext dsl, CollectionData collectionData) throws BackendException, RollbackException {
        Iterator<DocPartData> docPartDataIterator = StreamSupport.stream(collectionData.spliterator(), false)
                .iterator();
        List<Integer> generatedDids = new ArrayList<>();
        while (docPartDataIterator.hasNext()) {
            DocPartData docPartData = docPartDataIterator.next();
            if (docPartData.getMetaDocPart().getTableRef().isRoot()) {
                docPartData.forEach(docPartRow -> {
                    generatedDids.add(docPartRow.getDid());
                });
            }
            databaseInterface.insertDocPartData(dsl, schema.databaseSchemaName, docPartData);
        }
        return generatedDids;
    }

    protected CollectionData writeDocumentMeta(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, KVDocument document)
            throws Exception {
    	return writeDocumentsMeta(mutableSnapshot, dsl, Arrays.asList(document));
    }

    protected CollectionData writeDocumentsMeta(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, List<KVDocument> documents)
            throws Exception {
        CollectionData collectionData = readDataFromDocuments(schema.databaseName, schema.collectionName, documents, mutableSnapshot);
        mutableSnapshot.streamMetaDatabases().forEachOrdered(metaDatabase -> {
            metaDatabase.streamMetaCollections().forEachOrdered(metaCollection -> {
                metaCollection.streamContainedMetaDocParts().sorted(TableRefComparator.MetaDocPart.ASC).forEachOrdered(metaDocPartObject -> {
                    MetaDocPart metaDocPart = (MetaDocPart) metaDocPartObject;
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
    
    protected KVDocument parseFromJson(String jsonFileName) throws Exception {
        return parser.createFromResource("docs/" + jsonFileName);
    }
    
    protected CollectionData readDataFromDocuments(String database, String collection, List<KVDocument> documents, MutableMetaSnapshot mutableSnapshot) throws Exception {
        MockRidGenerator ridGenerator = new MockRidGenerator();
        IdentifierFactory identifierFactory = new IdentifierFactoryImpl();
        
        D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, mutableSnapshot, database, collection);
        for (KVDocument document : documents) {
            translator.translate(document);
        }
        return translator.getCollectionDataAccumulator();
    }
    
}
