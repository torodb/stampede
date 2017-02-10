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
import com.eightkdata.mongowp.bson.*;
import com.eightkdata.mongowp.bson.impl.*;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Parameter;
import java.time.Clock;
import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
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

  @Parameterized.Parameters(name = "{index}: Collection {0}")
  public static Collection<Object[]> data() {


    Collection<Object[]> allTests = Arrays.asList(new Object[][] {
            {"DOUBLE", PrimitiveBsonDouble.newInstance(2.3)},
            {"STRING", new StringBsonString("hello")},
            {"DOCUMENT", new SingleEntryBsonDocument("salutation", new StringBsonString("hello"))},
            {"ARRAY", new SingleValueBsonArray(new StringBsonString("hello"))},
            {"BINARY", new ByteArrayBsonBinary(BinarySubtype.USER_DEFINED, Byte.parseByte("1000", 2), new byte [1])},
            {"UNDEFINED", SimpleBsonUndefined.getInstance()},
            {"OBJECT_ID", new IntBasedBsonObjectId(1, 1, 1, 1)},
            {"BOOLEAN", TrueBsonBoolean.getInstance()},
            {"DATETIME", new LongBsonDateTime(0L)},
            {"NULL", SimpleBsonNull.getInstance()},
            {"REGEX", new DefaultBsonRegex(EnumSet.noneOf(BsonRegex.Options.class), "asd")},
            {"DB_POINTER", new DefaultBsonDbPointer("asd", new IntBasedBsonObjectId(1, 1, 1, 1))},
            {"JAVA_SCRIPT", new DefaultBsonJavaScript("alert(\"hello\");")},
            {"DEPRECATED", new StringBsonDeprecated("Deprecable me")},
            {"JAVA_SCRIPT_WITH_SCOPE", new DefaultBsonJavaScriptWithCode("alert(\"hello\");", new SingleEntryBsonDocument("class", new StringBsonString(".main-menu")))},
            {"INT32", PrimitiveBsonInt32.newInstance(55)},
            {"TIMESTAMP", new DefaultBsonTimestamp(1482000000, 1)},
            {"INT64", PrimitiveBsonInt64.newInstance(1525155)},
            {"DECIMAL128", new LongsBsonDecimal128(1L,1L)},
            {"DECIMAL128_NaN", new LongsBsonDecimal128(8935141660703064064L,1L)},
            {"DECIMAL128_INFINITE", new LongsBsonDecimal128(8646911284551352320L,1L)},
            {"MIN", SimpleBsonMin.getInstance()},
            {"MAX", SimpleBsonMax.getInstance()}
    });

    Arrays.asList(BsonType.values()).forEach(
            type -> assertTrue(type + " type is never tested",allTests.stream().anyMatch(
                    toTest -> type.getValueClass().isAssignableFrom(toTest[1].getClass())
                    )
            )
    );

    return allTests;
  }

  @Parameterized.Parameter(0)
  public String collName;


  @Parameterized.Parameter(1)
  public BsonValue value;

  @Test
  public void test() {
    List<BsonDocument> docs = new ArrayList<>();
    docs.add(new SingleEntryBsonDocument("number", value));

    baseWriteTest(docs, collName);
  }

  private void baseWriteTest(List<BsonDocument> docs, String collName) {

    writeTransaction = bundle.getExternalInterface().getMongodServer().openConnection().openWriteTransaction();

    //Prepare and do the insertion

    InsertCommand.InsertArgument insertArgument = new InsertCommand.InsertArgument.Builder(collName)
            .addDocuments(docs)
            .build();
    Status<InsertCommand.InsertResult> insertResultStatus =
            writeTransaction.execute(request, InsertCommand.INSTANCE, insertArgument);

    //Prepare and do the retrieval

    FindCommand.FindArgument findArgument = new FindCommand.FindArgument.Builder()
            .setCollection(collName)
            .build();
    Status<FindCommand.FindResult> findResultStatus =
            writeTransaction.execute(request, FindCommand.INSTANCE, findArgument);

    writeTransaction.close();


    //Asserting that the status of both operations are right

    assertTrue(insertResultStatus.getResult().getWriteErrors().isEmpty());
    assertTrue(insertResultStatus.isOk()?"ok":insertResultStatus.getErrorMsg() + " in collection " + collName, insertResultStatus.isOk());
    assertTrue(findResultStatus.isOk()?"ok":findResultStatus.getErrorMsg() + " in collection " + collName, findResultStatus.isOk());

    //We ensure that every element in the inserted list exists in the list retrieved from DB and viceversa
    //That is, we ensure that (apart from the order) they are the same

    BsonArray result = findResultStatus.getResult().getCursor().marshall(elm -> elm).get("firstBatch").asArray();

    result.forEach(doc -> assertTrue(docs.contains(doc)));

    docs.forEach(doc -> assertTrue(result.contains(doc)));
  }
}
