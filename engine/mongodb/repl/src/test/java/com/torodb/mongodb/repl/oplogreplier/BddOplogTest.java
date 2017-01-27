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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.google.common.truth.Truth;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.ReadOnlyMongodTransaction;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.OplogTest;
import com.torodb.mongodb.repl.oplogreplier.OplogTestContext;
import com.torodb.torod.TorodTransaction;
import org.junit.Assert;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

public abstract class BddOplogTest implements OplogTest {

  protected abstract Collection<DatabaseState> getInitialState();

  protected abstract Collection<DatabaseState> getExpectedState();

  protected abstract Stream<OplogOperation> streamOplog();

  @Nullable
  protected abstract Class<? extends Exception> getExpectedExceptionClass();

  @Override
  public void execute(OplogTestContext context) throws Exception {
    MongodServer server = context.getMongodServer();

    try (MongodConnection conn = server.openConnection()) {
      try (WriteMongodTransaction trans = conn.openWriteTransaction(true)) {
        given(trans);
        trans.commit();
      }
    }

    Exception error = null;
    try {
      context.apply(streamOplog(), getApplierContext());
    } catch (Exception t) {
      error = t;
    }

    try (MongodConnection conn = server.openConnection()) {
      try (ReadOnlyMongodTransaction trans = conn.openReadOnlyTransaction()) {
        then(trans, error);
      }
    }
  }

  protected ApplierContext getApplierContext() {
    return new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();
  }

  @Override
  public Optional<String> getTestName() {
    return Optional.of(this.getClass().getSimpleName());
  }

  @Override
  public boolean shouldIgnore() {
    return false;
  }

  protected void given(WriteMongodTransaction trans) throws Exception {
    for (DatabaseState db : getInitialState()) {
      String dbName = db.getName();
      for (CollectionState col : db.getCollections()) {
        String colName = col.getName();
        trans.getTorodTransaction().insert(dbName, colName,
            col.getDocs().stream());
      }
    }
  }

  protected void then(ReadOnlyMongodTransaction trans, Exception error) throws Exception {
    checkError(error);

    Collection<DatabaseState> expectedState = getExpectedState();

    TorodTransaction torodTrans = trans.getTorodTransaction();
    for (DatabaseState db : expectedState) {
      String dbName = db.getName();
      for (CollectionState col : db.getCollections()) {
        String colName = col.getName();

        Map<KvValue<?>, KvDocument> storedDocs = torodTrans
            .findAll(dbName, colName)
            .asDocCursor()
            .transform(toroDoc -> toroDoc.getRoot())
            .getRemaining()
            .stream()
            .collect(Collectors.toMap(
                doc -> doc.get("_id"),
                doc -> doc)
            );

        for (KvDocument expectedDoc : col.getDocs()) {
          KvValue<?> id = expectedDoc.get("_id");
          assert id != null : "The test is incorrect, as " + expectedDoc + " does not have _id";

          KvDocument storedDoc = storedDocs.get(id);
          assertTrue("It was expected to have a document with _id " + id,
              storedDoc != null);
          assertEquals("The found document is different than expected",
              expectedDoc, storedDoc);
        }

        assertEquals("Unexpected size on " + dbName + "." + colName,
            col.getDocs().size(),
            storedDocs.size());
      }
    }

    Set<String> foundNs = torodTrans.getDatabases().stream()
        .filter(dbName -> !dbName.equals("torodb"))
        .flatMap(dbName -> torodTrans.getCollectionsInfo(dbName)
            .map(colInfo -> dbName + '.' + colInfo.getName())
        ).collect(Collectors.toSet());
    Set<String> expectedNs = expectedState.stream()
        .flatMap(db -> db.getCollections().stream()
            .map(col -> db.getName() + '.' + col.getName())
        ).collect(Collectors.toSet());

    Truth.assertWithMessage("Unexpected namespaces")
        .that(foundNs)
        .containsExactlyElementsIn(expectedNs);

    Set<String> expectedDbNames = expectedState.stream()
        .map(DatabaseState::getName)
        .collect(Collectors.toSet());
    Set<String> foundDbNames = trans.getTorodTransaction()
        .getDatabases()
        .stream()
        .filter(dbName -> !dbName.equals("torodb"))
        .collect(Collectors.toSet());
    Truth.assertWithMessage("Unexpected databases")
        .that(foundDbNames)
        .containsExactlyElementsIn(expectedDbNames);
  }

  private void checkError(Exception error) throws Exception {
    Class<? extends Throwable> expectedThrowableClass = getExpectedExceptionClass();
    if (error == null) { //no error found on the test
      if (expectedThrowableClass == null) { //no error was expected
        return; //everything is fine
      } else {
        Assert.fail("The execution completed successfully, but a "
            + expectedThrowableClass.getSimpleName() + " was expected to be thrown");
      }
    } else {
      if (expectedThrowableClass == null) {
        throw error;
      } else {
        if (expectedThrowableClass.isAssignableFrom(error.getClass())) {
          throw new AssertionError("It was expected that the execution throws a "
              + expectedThrowableClass.getName() + " but " + error + " was found", error);
        }
      }
    }
  }

  public static class DatabaseState {

    private final String name;
    private final Collection<CollectionState> collections;

    public DatabaseState(String name, Stream<CollectionState> cols) {
      this.name = name;
      this.collections = cols.collect(Collectors.toList());
    }

    public String getName() {
      return name;
    }

    public Collection<CollectionState> getCollections() {
      return collections;
    }
  }

  public static class CollectionState {

    private final String name;
    private final Set<KvDocument> docs;

    public CollectionState(String name, Stream<KvDocument> docs) {
      this.name = name;
      this.docs = docs.collect(Collectors.toSet());
    }

    public String getName() {
      return name;
    }

    public Set<KvDocument> getDocs() {
      return docs;
    }
  }

}
