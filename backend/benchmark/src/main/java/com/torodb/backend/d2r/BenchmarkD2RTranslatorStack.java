package com.torodb.backend.d2r;

import com.torodb.backend.util.InMemoryRidGenerator;
import com.torodb.backend.util.TestDataFactory;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockIdentifierInterface;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;
import java.util.ArrayList;
import java.util.List;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import static com.torodb.backend.util.TestDataFactory.*;

public class BenchmarkD2RTranslatorStack {

    private static TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    private static InMemoryRidGenerator ridGenerator = new InMemoryRidGenerator();
	private static IdentifierFactory identifierFactory=new IdentifierFactoryImpl(new MockIdentifierInterface());
	
	@State(Scope.Thread)
	public static class TranslateState {
		
		public List<KVDocument> document;

		@Setup(Level.Iteration)
		public void setup(){
			document=new ArrayList<>();
			for (int i = 0; i<100; i++) {
				document.add(TestDataFactory.buildDoc());
			}
		}
	}

	@Benchmark
	@Fork(value=5)
	@BenchmarkMode(value=Mode.Throughput)
	@Warmup(iterations=3)
	@Measurement(iterations=10) 
	public void benchmarkTranslate(TranslateState state, Blackhole blackhole) {
		MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(InitialView);
		MutableMetaSnapshot mutableSnapshot;
		try (SnapshotStage snapshot = mvccMetainfoRepository.startSnapshotStage()) {
			mutableSnapshot = snapshot.createMutableSnapshot();
		}
        MutableMetaDatabase db = mutableSnapshot.getMetaDatabaseByName(DB1);
		D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, db, db.getMetaCollectionByName(COLL1));
		for(KVDocument doc: state.document){
			translator.translate(doc);
		}
		blackhole.consume(translator.getCollectionDataAccumulator());
	}
	
}
