package com.torodb.backend.d2r;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.torodb.backend.*;
import com.torodb.backend.derby.guice.DerbyBackendModule;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.util.InMemoryRidGenerator;
import com.torodb.backend.util.TestDataFactory;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.d2r.*;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.guice.CoreModule;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockIdentifierInterface;
import com.torodb.d2r.R2DTranslatorImpl;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;
import javax.inject.Singleton;
import org.jooq.DSLContext;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import static com.torodb.backend.util.TestDataFactory.*;

public class BenchmarkDerbyR2DBackedTranslator {

    private static TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    private static InMemoryRidGenerator ridGenerator = new InMemoryRidGenerator();
	private static IdentifierFactory identifierFactory=new IdentifierFactoryImpl(new MockIdentifierInterface());
	
	@State(Scope.Thread)
	public static class TranslateState {
		
        public List<KVDocument> documents;
        public SqlInterface sqlInterface;
        public Connection connection;
        public DSLContext dsl;
        public MutableMetaSnapshot mutableSnapshot;
        public MutableMetaDatabase metaDatabase;
        public MutableMetaCollection metaCollection;
        public Iterator<DocPartResult> docPartResultSets;
        public R2DTranslator r2dTranslator;

		@Setup(Level.Invocation)
		public void setup() throws Exception {
		    Injector injector = createInjector();
	        DbBackendService dbBackend = injector.getInstance(DbBackendService.class);
	        dbBackend.startAsync();
	        dbBackend.awaitRunning();
            sqlInterface = injector.getInstance(SqlInterface.class);
            SqlHelper sqlHelper = injector.getInstance(SqlHelper.class);
		    cleanDatabase(dbBackend, sqlInterface.getIdentifierConstraints());
		    connection = sqlInterface.getDbBackend().createWriteConnection();
		    dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
			documents=new ArrayList<>();
			for (int i=0;i<100;i++){
				documents.add(TestDataFactory.buildDoc());
			}
	        MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(InitialView);
	        try (SnapshotStage snapshot = mvccMetainfoRepository.startSnapshotStage()) {
	            mutableSnapshot = snapshot.createMutableSnapshot();
	        }
            metaDatabase = mutableSnapshot.getMetaDatabaseByName(DB1);
            metaCollection = metaDatabase.getMetaCollectionByName(COLL1);
            resetDatabase(this);
	        writeDocumentsMeta(this, null);
	        connection.commit();
	        CollectionData collectionData = readDataFromDocuments(this, null);
	        List<Integer> writtenDocs = writeCollectionData(this, null, collectionData);
	        docPartResultSets = sqlInterface.getReadInterface().getCollectionResultSets(
	                dsl, metaDatabase, metaCollection, 
	                new MockDidCursor(writtenDocs.iterator()), writtenDocs.size());
	        r2dTranslator = new R2DTranslatorImpl();
		}
		
	    public Injector createInjector() {
	        return Guice.createInjector(
	                    new CoreModule(),
	                    new Module() {
	                        @Override
	                        public void configure(Binder binder) {
	                            binder.bind(SqlInterface.class)
	                                .to(SqlInterfaceDelegate.class)
	                                .in(Singleton.class);
	                            binder.bind(DslContextFactory.class)
	                                .to(DslContextFactoryImpl.class)
	                                .asEagerSingleton();
	                        }
	                    },
	                    new DerbyBackendModule(),
	                    getConfigurationModule());
	    }

	    public Module getConfigurationModule() {
	        return new Module() {
	            @Override
	            public void configure(Binder binder) {
	                binder.bind(DerbyDbBackendConfiguration.class)
	                    .toInstance(getBackEndConfiguration());
	            }
	        };
	    }
	    
	    public DerbyDbBackendConfiguration getBackEndConfiguration(){
	        return new DerbyDbBackendConfiguration() {
	            @Override
	            public String getUsername() {
	                return null;
	            }
	            
	            @Override
	            public int getReservedReadPoolSize() {
	                return 4;
	            }
	            
	            @Override
	            public String getPassword() {
	                return null;
	            }
	            
	            @Override
	            public int getDbPort() {
	                return 0;
	            }
	            
	            @Override
	            public String getDbName() {
	                return "torodb";
	            }
	            
	            @Override
	            public String getDbHost() {
	                return null;
	            }
	            
	            @Override
	            public long getCursorTimeout() {
	                return 8000;
	            }
	            
	            @Override
	            public long getConnectionPoolTimeout() {
	                return 10000;
	            }
	            
	            @Override
	            public int getConnectionPoolSize() {
	                return 8;
	            }

	            @Override
	            public boolean inMemory() {
	                return true;
	            }

	            @Override
	            public boolean embedded() {
	                return true;
	            }
	        };
	    }
	    
	    public static void cleanDatabase(DbBackend dbBackend, IdentifierConstraints identifierConstraints) throws SQLException {
	        try (Connection connection = dbBackend.createSystemConnection()) {
	            DatabaseMetaData metaData = connection.getMetaData();
	            ResultSet tables = metaData.getTables("%", "%", "%", null);
	            while (tables.next()) {
	                String schemaName = tables.getString("TABLE_SCHEM");
	                String tableName = tables.getString("TABLE_NAME");
	                if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.IDENTIFIER)) {
	                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
	                        preparedStatement.executeUpdate();
	                    }
	                }
	            }
	            ResultSet schemas = metaData.getSchemas();
	            while (schemas.next()) {
	                String schemaName = schemas.getString("TABLE_SCHEM");
	                if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.IDENTIFIER)) {
	                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP SCHEMA \"" + schemaName + "\" RESTRICT")) {
	                        preparedStatement.executeUpdate();
	                    }
	                }
	            }
	            connection.commit();
	        }
	    }
    }

	@Benchmark
	@Fork(value=5)
	@BenchmarkMode(value=Mode.Throughput)
	@Warmup(iterations=3)
	@Measurement(iterations=10) 
	public void benchmarkTranslate(TranslateState state, Blackhole blackhole) throws Exception {
	    Collection<ToroDocument> readedDocuments = readDocuments(state, blackhole);
	    blackhole.consume(readedDocuments);
	}
    
    @SuppressWarnings({ "unchecked" })
    protected static Collection<ToroDocument> readDocuments(TranslateState state, Blackhole blackhole) {
        Collection<ToroDocument> readedDocuments = state.r2dTranslator.translate(state.docPartResultSets);
        return readedDocuments;
    }

    protected static CollectionData writeDocumentsMeta(TranslateState state, Blackhole blackhole)
            throws Exception {
        CollectionData collectionData = readDataFromDocuments(state, blackhole);
        state.mutableSnapshot.streamMetaDatabases().forEachOrdered(metaDatabase -> {
            metaDatabase.streamMetaCollections().forEachOrdered(metaCollection -> {
                metaCollection.streamContainedMetaDocParts().sorted(TableRefComparator.MetaDocPart.ASC).forEachOrdered(metaDocPartObject -> {
                    MetaDocPart metaDocPart = (MetaDocPart) metaDocPartObject;
                    state.sqlInterface.getStructureInterface().createRootDocPartTable(state.dsl, metaDatabase.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef());
                    metaDocPart.streamFields().forEachOrdered(metaField -> {
                        state.sqlInterface.getStructureInterface().addColumnToDocPartTable(state.dsl, metaDatabase.getIdentifier(), metaDocPart.getIdentifier(), 
                                metaField.getIdentifier(), state.sqlInterface.getDataTypeProvider().getDataType(metaField.getType()));
                    });
                });
            });
        });
        return collectionData;
    }

    protected static List<Integer> writeCollectionData(TranslateState state, Blackhole blackhole, CollectionData collectionData) {
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
            state.sqlInterface.getWriteInterface().insertDocPartData(state.dsl, DB1, docPartData);
        }
        return generatedDids;
    }
    
    protected static CollectionData readDataFromDocuments(TranslateState state, Blackhole blackhole) throws Exception {
        D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, 
                state.metaDatabase, state.metaCollection);
        for (KVDocument document : state.documents) {
            translator.translate(document);
        }
        return translator.getCollectionDataAccumulator();
    }

    protected static void resetDatabase(TranslateState state) throws SQLException {
        DatabaseMetaData metaData = state.connection.getMetaData();
        ResultSet tables = metaData.getTables("%", "%", "%", null);
        while (tables.next()) {
            String schemaName = tables.getString("TABLE_SCHEM");
            String tableName = tables.getString("TABLE_NAME");
            if (!state.sqlInterface.getIdentifierConstraints().isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.IDENTIFIER)) {
                try (PreparedStatement preparedStatement = state.connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
                    preparedStatement.executeUpdate();
                }
            }
        }
        ResultSet schemas = metaData.getSchemas();
        while (schemas.next()) {
            String schemaName = schemas.getString("TABLE_SCHEM");
            if (!state.sqlInterface.getIdentifierConstraints().isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.IDENTIFIER)) {
                try (PreparedStatement preparedStatement = state.connection.prepareStatement("DROP SCHEMA \"" + schemaName + "\" RESTRICT")) {
                    preparedStatement.executeUpdate();
                }
            }
        }
        state.connection.commit();
    }
	
}
