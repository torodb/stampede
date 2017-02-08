/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonInt32;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.impl.ListBasedBsonDocument;
import com.eightkdata.mongowp.bson.impl.PrimitiveBsonInt32;
import com.eightkdata.mongowp.bson.impl.SingleEntryBsonDocument;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.NameBasedCommandLibrary;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.modules.BundleConfigImpl;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.engine.essential.DefaultBuildProperties;
import com.torodb.engine.essential.EssentialModule;
import com.torodb.mongodb.commands.impl.CommandClassifierImpl;
import com.torodb.mongodb.commands.impl.EmptyCommandClassifier;
import com.torodb.mongodb.commands.signatures.general.FindCommand;
import com.torodb.mongodb.commands.signatures.general.InsertCommand;
import com.torodb.torod.MemoryTorodBundle;
import com.torodb.torod.TorodBundle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class MongoDbBsonTypesTest {

  private BundleConfig generalConfig;
  private TorodBundle torodBundle;
  private MongoDbCoreBundle bundle;
  private Request request;
  private final String dbName="test";
  private WriteMongodTransaction writeTransaction;

  @Before
  public void setUp() {
    Supervisor supervisor = new Supervisor() {
      @Override
      public SupervisorDecision onError(Object supervised, Throwable error) {
        throw new AssertionError("error on " + supervised, error);
      }
    };
    Injector essentialInjector = Guice.createInjector(
        new EssentialModule(
            () -> true,
            Clock.systemUTC()
        )
    );

    generalConfig = new BundleConfigImpl(essentialInjector, supervisor);
    torodBundle = new MemoryTorodBundle(generalConfig);

    torodBundle.startAsync();
    torodBundle.awaitRunning();

    MongoDbCoreConfig config = new MongoDbCoreConfig(torodBundle,
        new NameBasedCommandLibrary("test", ImmutableMap.of()),
        CommandClassifierImpl.createDefault(Clock.systemUTC(), new DefaultBuildProperties(), new
                MongodServerConfig(HostAndPort.fromParts("localhost",8095))), essentialInjector, supervisor);

    bundle = new MongoDbCoreBundle(config);

    request = new Request(dbName, null, true, null);

    bundle.start();
  }

  @After
  public void tearDown() {
    bundle.stop();

    if (torodBundle != null && torodBundle.isRunning()) {
      torodBundle.stopAsync();
    }
  }

  @Test
  public void test() {

    BsonDocument doc = new SingleEntryBsonDocument("number", PrimitiveBsonInt32.newInstance(55));
    baseWriteTest(doc, "int32");

  }

  private void baseWriteTest(BsonDocument doc, String collName) {
    List<BsonDocument> docs = new ArrayList<>();
    docs.add(doc);

    baseWriteTest(docs, collName);
  }

  private void baseWriteTest(List<BsonDocument> docs, String collName) {

    writeTransaction = bundle.getExternalInterface().getMongodServer().openConnection().openWriteTransaction();

    InsertCommand.InsertArgument insertArgument = new InsertCommand.InsertArgument(collName,
            docs, WriteConcern.acknowledged(), true, null);

    FindCommand.FindArgument findArgument = new FindCommand.FindArgument.Builder()
            .setCollection(collName)
            .build();

    Status<InsertCommand.InsertResult> insertResultStatus = writeTransaction.execute(request, InsertCommand.INSTANCE, insertArgument);

    Status<FindCommand.FindResult> findResultStatus = writeTransaction.execute(request, FindCommand.INSTANCE, findArgument);

    writeTransaction.close();

    //Asserting that the status of both operations are right

    assertTrue(insertResultStatus.isOk()?"ok":insertResultStatus.getErrorMsg() + " in collection " + collName, insertResultStatus.isOk());
    assertTrue(insertResultStatus.getResult().getWriteErrors().isEmpty());
    assertTrue(findResultStatus.isOk()?"ok":findResultStatus.getErrorMsg() + " in collection " + collName, findResultStatus.isOk());

    //We ensure that every element in the inserted list exists in the list retrieved from DB and viceversa
    //That is, we ensure that (apart from the order) they are the same

    BsonArray result = findResultStatus.getResult().getCursor().marshall(elm -> elm).get("firstBatch").asArray();

    result.stream().forEach(doc ->{
      System.out.println(doc);
      assertTrue(docs.contains(doc));
    });

    docs.stream().forEach(doc ->{
      System.out.println(doc);
      assertTrue(result.contains(doc));
    });
  }
}
