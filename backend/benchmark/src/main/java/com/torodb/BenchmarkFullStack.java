/*
 * MongoWP - ToroDB-poc: Backends benchmark
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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

import com.torodb.backend.util.TestDataFactory;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.packaging.config.model.backend.postgres.AbstractPostgres;
import com.torodb.standalone.ToroDbStandaloneTestUtil;
import com.torodb.standalone.ToroDbStandaloneTestUtil.TestService;
import com.torodb.standalone.config.model.Config;
import com.torodb.torod.SharedWriteTorodTransaction;
import com.torodb.torod.TorodConnection;
import com.torodb.torod.TorodServer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import com.torodb.torod.SharedWriteTorodTransaction;

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
                config.getBackend().as(AbstractPostgres.class).setPassword("torodb");
                TestService testService = ToroDbStandaloneTestUtil.createInjectors(config, Clock.systemDefaultZone());
                torod = testService.getInjector().getInstance(TorodServer.class);
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
	    	try(SharedWriteTorodTransaction toroTransaction = toroConnection.openWriteTransaction(true)){
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
	    	try(SharedWriteTorodTransaction toroTransaction = toroConnection.openWriteTransaction(false)){
	            toroTransaction.insert("test", "test", state.documents.stream());
	            toroTransaction.commit();
	    	}
	    }
	}
	
}
