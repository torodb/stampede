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

package com.torodb.mongodb.repl.oplogreplier;


import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonInt32;
import com.eightkdata.mongowp.bson.BsonString;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.org.bson.utils.MongoBsonTranslator;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.google.common.base.Charsets;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.mongodb.commands.pojos.OplogOperationParser;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.BddOplogTest;
import com.torodb.mongodb.repl.oplogreplier.BddOplogTest.CollectionState;
import com.torodb.mongodb.repl.oplogreplier.BddOplogTest.DatabaseState;
import com.torodb.mongodb.repl.oplogreplier.UnexpectedOplogOperationException;
import org.jooq.lambda.Unchecked;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

/**
 *
 */
public class OplogTestParser {

  public static BddOplogTest fromExtendedJsonResource(String resourceName) throws IOException {
    String text;
    try (InputStream resourceAsStream = OplogTestParser.class.getResourceAsStream(resourceName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream))) {
      text = reader.lines().collect(Collectors.joining("\n"));
    }
    return fromExtendedJsonString(text);
  }

  public static BddOplogTest fromExtendedJsonFile(File f) throws IOException {
    String text = new String(Files.readAllBytes(Paths.get(f.toURI())), Charsets.UTF_8);
    return fromExtendedJsonString(text);
  }

  public static BddOplogTest fromExtendedJsonString(String text) {
    BsonDocument doc = MongoBsonTranslator.translate(
        org.bson.BsonDocument.parse(text)
    );

    return fromDocument(doc);
  }

  public static BddOplogTest fromDocument(BsonDocument doc) {
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();
    return new ParsedOplogTest(getTestName(doc), getIgnore(doc),
        getInitialState(doc), getExpectedState(doc), getOps(doc),
        getExpectedException(doc).orElse(null), applierContext);
  }

  private static Collection<DatabaseState> getInitialState(BsonDocument root) {
    BsonValue<?> value = root.get("initialState");
    if (value == null) {
      throw new AssertionError("Does not contain initialState");
    }
    return getState(value);
  }

  private static Collection<DatabaseState> getExpectedState(BsonDocument root) {
    BsonValue<?> value = root.get("expectedState");
    if (value == null) {
      throw new AssertionError("Does not contain expectedState");
    }
    return getState(value);
  }

  private static Collection<DatabaseState> getState(BsonValue<?> stateValue) {
    return stateValue.asDocument().stream()
        .map(OplogTestParser::parseDatabase)
        .collect(Collectors.toList());
  }

  private static DatabaseState parseDatabase(BsonDocument.Entry<?> entry) {
    return new DatabaseState(entry.getKey(), entry.getValue().asDocument()
        .stream()
        .map(OplogTestParser::parseCollection)
    );
  }

  private static CollectionState parseCollection(BsonDocument.Entry<?> entry) {
    return new CollectionState(entry.getKey(), entry.getValue().asArray()
        .stream()
        .map(MongoWpConverter::translate)
        .map(kvValue -> {
          return (KvDocument) kvValue;
        })
    );
  }

  private static List<OplogOperation> getOps(BsonDocument doc) {
    BsonValue<?> oplogValue = doc.get("oplog");
    if (oplogValue == null) {
      throw new AssertionError("Does not contain oplog");
    }
    AtomicInteger tsFactory = new AtomicInteger();
    AtomicInteger tFactory = new AtomicInteger();
    BsonInt32 twoInt32 = DefaultBsonValues.newInt(2);

    return oplogValue.asArray().asList().stream()
        .map(BsonValue::asDocument)
        .map(child -> {
          BsonDocumentBuilder builder = new BsonDocumentBuilder(child);
          if (child.get("ts") == null) {
            builder.appendUnsafe("ts", DefaultBsonValues.newTimestamp(
                tsFactory.incrementAndGet(),
                tFactory.incrementAndGet())
            );
          }
          if (child.get("h") == null) {
            builder.appendUnsafe("h", DefaultBsonValues.INT32_ONE);
          }
          if (child.get("v") == null) {
            builder.appendUnsafe("v", twoInt32);
          }
          return builder.build();
        })
        .map(child -> {
          try {
            return OplogOperationParser.fromBson(child);
          } catch (MongoException ex) {
            throw new AssertionError("Invalid oplog operation", ex);
          }
        })
        .collect(Collectors.toList());
  }

  private static Optional<Class<? extends Exception>> getExpectedException(BsonDocument doc) {
    @SuppressWarnings("unchecked")
    Function<String, Class<? extends Exception>> toClazz = Unchecked.function(name ->
        (Class<? extends Exception>) Class.forName(name)
    );
    return doc.getOptional("expectedException")
        .map(BsonValue::asString)
        .map(BsonString::getValue)
        .map(OplogTestParser::exceptionAlias)
        .map(toClazz);
  }

  private static String exceptionAlias(String exceptionName) {
    if (exceptionName.equals("UnexpectedOplogApplierException")) {
      return UnexpectedOplogOperationException.class.getCanonicalName();
    }
    return exceptionName;
  }

  private static boolean getIgnore(BsonDocument doc) {
    BsonValue<?> ignoreValue = doc.get("ignore");
    return ignoreValue != null && ignoreValue.asBoolean().getPrimitiveValue();
  }

  private static Optional<String> getTestName(BsonDocument doc) {
    BsonValue<?> nameValue = doc.get("name");
    return Optional.ofNullable(nameValue).map(value -> value.asString().getValue());
  }

  private static class ParsedOplogTest extends BddOplogTest {

    private final Optional<String> name;
    private final boolean ignore;
    private final Collection<DatabaseState> initialState;
    private final Collection<DatabaseState> expectedState;
    @Nullable
    private final Class<? extends Exception> expectedThrowableClass;
    private final List<OplogOperation> oplogOps;
    private final ApplierContext applierContext;

    public ParsedOplogTest(Optional<String> name, boolean ignore,
        Collection<DatabaseState> initialState,
        Collection<DatabaseState> expectedState,
        List<OplogOperation> oplogOps,
        @Nullable Class<? extends Exception> expectedThrowableClass,
        ApplierContext applierContext) {
      this.initialState = initialState;
      this.expectedState = expectedState;
      this.oplogOps = oplogOps;
      this.expectedThrowableClass = expectedThrowableClass;
      this.applierContext = applierContext;
      this.name = name;
      this.ignore = ignore;
    }

    @Override
    protected Collection<DatabaseState> getInitialState() {
      return initialState;
    }

    @Override
    protected Collection<DatabaseState> getExpectedState() {
      return expectedState;
    }

    @Override
    protected Class<? extends Exception> getExpectedExceptionClass() {
      return expectedThrowableClass;
    }

    @Override
    public Optional<String> getTestName() {
      return name;
    }

    @Override
    public boolean shouldIgnore() {
      return ignore;
    }

    @Override
    public ApplierContext getApplierContext() {
      return applierContext;
    }

    @Override
    public Stream<OplogOperation> streamOplog() {
      return oplogOps.stream();
    }

  }

}
