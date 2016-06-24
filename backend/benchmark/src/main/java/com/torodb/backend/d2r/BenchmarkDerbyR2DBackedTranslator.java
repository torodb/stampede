package com.torodb.backend.d2r;

import static com.torodb.backend.util.TestDataFactory.COLL1;
import static com.torodb.backend.util.TestDataFactory.DB1;
import static com.torodb.backend.util.TestDataFactory.InitialView;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.inject.Injector;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.MockDidCursor;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.TableRefComparator;
import com.torodb.backend.derby.Derby;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.util.InMemoryRidGenerator;
import com.torodb.backend.util.TestDataFactory;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockIdentifierInterface;
import com.torodb.d2r.R2DBackedTranslator;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

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
        public DocPartResults<ResultSet> docPartResultSets;
        public R2DTranslator r2dTranslator;

		@Setup(Level.Invocation)
		public void setup() throws Exception {
		    Injector injector = Derby.createInjector();
	        DbBackendService dbBackend = injector.getInstance(DbBackendService.class);
	        dbBackend.startAsync();
	        dbBackend.awaitRunning();
		    Derby.cleanDatabase(injector);
		    sqlInterface = injector.getInstance(SqlInterface.class);
		    connection = sqlInterface.createWriteConnection();
		    dsl = sqlInterface.createDSLContext(connection);
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
	        docPartResultSets = sqlInterface.getCollectionResultSets(
	                dsl, metaDatabase, metaCollection, 
	                new MockDidCursor(writtenDocs.iterator()), writtenDocs.size());
	        r2dTranslator = new R2DBackedTranslator(new R2DBackendTranslatorImpl(sqlInterface, metaDatabase, metaCollection));
		}
	}

	@Benchmark
	@Fork(value=5)
	@BenchmarkMode(value=Mode.Throughput)
	@Warmup(iterations=3)
	@Measurement(iterations=10) 
	public void benchmarkTranslate(TranslateState state, Blackhole blackhole) throws Exception {
	    Collection<KVDocument> readedDocuments = readDocuments(state, blackhole);
	    blackhole.consume(readedDocuments);
	}
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static Collection<KVDocument> readDocuments(TranslateState state, Blackhole blackhole) {
        Collection<KVDocument> readedDocuments = state.r2dTranslator.translate(state.docPartResultSets);
        return readedDocuments;
    }

    protected static CollectionData writeDocumentsMeta(TranslateState state, Blackhole blackhole)
            throws Exception {
        CollectionData collectionData = readDataFromDocuments(state, blackhole);
        state.mutableSnapshot.streamMetaDatabases().forEachOrdered(metaDatabase -> {
            metaDatabase.streamMetaCollections().forEachOrdered(metaCollection -> {
                metaCollection.streamContainedMetaDocParts().sorted(TableRefComparator.MetaDocPart.ASC).forEachOrdered(metaDocPartObject -> {
                    MetaDocPart metaDocPart = (MetaDocPart) metaDocPartObject;
                    List<Field<?>> fields = new ArrayList<>(state.sqlInterface.getDocPartTableInternalFields(metaDocPart));
                    metaDocPart.streamFields().forEachOrdered(metaField -> {
                        fields.add(DSL.field(metaField.getIdentifier(), state.sqlInterface.getDataType(metaField.getType())));
                    });
                    state.sqlInterface.createDocPartTable(state.dsl, metaDatabase.getIdentifier(), metaDocPart.getIdentifier(), fields);
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
            state.sqlInterface.insertDocPartData(state.dsl, DB1, docPartData);
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
            if (!state.sqlInterface.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                try (PreparedStatement preparedStatement = state.connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
                    preparedStatement.executeUpdate();
                }
            }
        }
        ResultSet schemas = metaData.getSchemas();
        while (schemas.next()) {
            String schemaName = schemas.getString("TABLE_SCHEM");
            if (!state.sqlInterface.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                try (PreparedStatement preparedStatement = state.connection.prepareStatement("DROP SCHEMA \"" + schemaName + "\" RESTRICT")) {
                    preparedStatement.executeUpdate();
                }
            }
        }
        state.connection.commit();
    }
	
}
