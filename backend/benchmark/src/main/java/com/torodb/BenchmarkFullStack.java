package com.torodb;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.inject.Injector;
import com.torodb.backend.util.TestDataFactory;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.packaging.ToroDbServer;
import com.torodb.packaging.config.model.Config;
import com.torodb.torod.TorodConnection;
import com.torodb.torod.TorodServer;
import com.torodb.torod.WriteTorodTransaction;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(
        value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
        justification = "State lifecycle is managed by JMH"
)
public class BenchmarkFullStack {
	
	@State(Scope.Thread)
	public static class FullStackState {
	    public TorodServer torod;
		
		public List<KVDocument> documents;

        @Setup(Level.Trial)
        public void startup() {
            if (torod == null) {
                Config config = new Config();
                config.getBackend().asPostgres().setPassword("torodb");
                Injector injector = ToroDbServer.createInjector(config, Clock.systemUTC());
                torod = injector.getInstance(TorodServer.class);
                torod.startAsync();
                torod.awaitRunning();
            }
        }

		@Setup(Level.Iteration)
		public void setup() {
			documents=new ArrayList<>();
			for (int i=0; i<1000; i++){
				documents.add(TestDataFactory.buildDoc());
			}
		}
		
		@TearDown
		public void teardown() {
		    torod.stopAsync();
		    torod.awaitTerminated();
		}
	}

	@Benchmark
	@Fork(value=5)
	@BenchmarkMode(value=Mode.Throughput)
	@Warmup(iterations=3)
	@Measurement(iterations=10) 
	public void benchmarkInsertConcurrent(FullStackState state, Blackhole blackhole) throws RollbackException, UserException {
	    try (TorodConnection toroConnection = state.torod.openConnection()) {
	    	try(WriteTorodTransaction toroTransaction = toroConnection.openWriteTransaction(true)){
	            toroTransaction.insert("test", "test", state.documents.stream());
	            toroTransaction.commit();
	    	}
	    }
	}

	@Benchmark
	@Fork(value=5)
	@BenchmarkMode(value=Mode.Throughput)
	@Warmup(iterations=3)
	@Measurement(iterations=10)
	public void benchmarkInsertSingleThread(FullStackState state, Blackhole blackhole) throws RollbackException, UserException {
	    try (TorodConnection toroConnection = state.torod.openConnection()) {
	    	try(WriteTorodTransaction toroTransaction = toroConnection.openWriteTransaction(false)){
	            toroTransaction.insert("test", "test", state.documents.stream());
	            toroTransaction.commit();
	    	}
	    }
	}
	
}
