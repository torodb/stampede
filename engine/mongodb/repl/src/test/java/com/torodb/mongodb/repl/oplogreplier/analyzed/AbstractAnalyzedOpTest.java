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

package com.torodb.mongodb.repl.oplogreplier.analyzed;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;
import static org.junit.Assert.*;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.server.api.oplog.*;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.StringKvString;
import com.torodb.mongodb.repl.oplogreplier.OpTimeFactory;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.function.Consumer3;
import org.junit.Test;

import java.util.Random;
import java.util.function.BiConsumer;

/**
 *
 */
public abstract class AbstractAnalyzedOpTest<E extends AnalyzedOp> {

  private static final Random RANDOM = new Random();
  private final KvValue<?> defaultMongoDocId = KvInteger.of(1);
  private static final OpTimeFactory opTimeFactory = new OpTimeFactory();

  abstract E getAnalyzedOp(KvValue<?> mongoDocId);

  abstract Logger getLogger();

  abstract void andThenInsertTest();

  abstract void andThenUpdateModTest();

  abstract void andThenUpdateSetTest();

  abstract void andThenUpsertModTest();

  abstract void andThenUpsertSetTest();

  abstract void andThenDeleteTest();

  public E getDefaultOp() {
    return getAnalyzedOp(defaultMongoDocId);
  }

  @Test
  public void getMismatchErrorMessageTest() {
    AnalyzedOp op = getAnalyzedOp(defaultMongoDocId);
    if (op.getType().requiresMatch()) {
      Status<?> status = op.getMismatchErrorMessage();
      assertNotNull(
          "The mismatch error of an analyzed operation that requires match must not be null", status);
      assertFalse("The mismatch error of an analyzed operation that requires match must not be OK",
          status.isOk());
    } else {
      try {
        op.getMismatchErrorMessage();
        fail("An analyzed operation that does not require match must throw a "
            + UnsupportedOperationException.class + " when its mismatch error is "
            + "requested");
      } catch (UnsupportedOperationException ex) {
      }
    }
  }

  @Test
  public void getMongoDocIdTest() {
    assertEquals("Unexpected mongo doc id.", defaultMongoDocId, getAnalyzedOp(defaultMongoDocId)
        .getMongoDocId());
  }

  @Test
  public void requiresToFetchToroIdTest() {
    AnalyzedOp op = getAnalyzedOp(defaultMongoDocId);
    assertEquals("", op.getType().requiresToFetchToroId(), op.requiresToFetchToroId());
  }

  @Test
  public void requiresMatchTest() {
    AnalyzedOp op = getAnalyzedOp(defaultMongoDocId);
    assertEquals("", op.getType().requiresMatch(), op.requiresMatch());
  }

  @Test
  public void requiresFetchTest() {
    AnalyzedOp op = getAnalyzedOp(defaultMongoDocId);
    assertEquals("", op.getType().requiresFetch(), op.requiresFetch());
  }

  @Test
  public void deletesTest() {
    AnalyzedOp op = getAnalyzedOp(defaultMongoDocId);
    assertEquals("", op.getType().deletes(), op.deletes());
  }

  //Functions to be called by the subclasses
  protected <Op extends CollectionOplogOperation> void emptyConsumer3(Op colOp, KvDocument newDoc,
      KvDocument matchDoc) {
    getLogger().warn("An empty check was done. It should be improved");
  }

  protected <Op extends CollectionOplogOperation> void emptyBiConsumer(Op colOp, KvDocument newDoc) {
    getLogger().warn("An empty check was done. It should be improved");
  }

  protected void andThenInsert(
      Consumer3<InsertOplogOperation, KvDocument, KvDocument> onMatchCallback,
      BiConsumer<InsertOplogOperation, KvDocument> onMismatchCallback) {

    AnalyzedOp op = getDefaultOp();
    AnalyzedOpType nextExpectedType = op.getType();

    InsertOplogOperation colOp = new InsertOplogOperation(
        (BsonDocument) MongoWpConverter.translate(createDefaultKVDoc(defaultMongoDocId)),
        "db",
        "col",
        opTimeFactory.newOpTime(),
        RANDOM.nextLong(),
        OplogVersion.V1,
        false
    );
    AnalyzedOp newOp = op.andThenInsert(colOp);

    andThenGeneric(colOp, newOp, nextExpectedType, onMatchCallback, onMismatchCallback);
  }

  protected void andThenUpdateMod(
      Consumer3<UpdateOplogOperation, KvDocument, KvDocument> onMatchCallback,
      BiConsumer<UpdateOplogOperation, KvDocument> onMismatchCallback) {

    AnalyzedOp op = getDefaultOp();
    AnalyzedOpType nextExpectedType = op.getType();

    UpdateOplogOperation colOp = createRandomUpdateModOplogOperation(defaultMongoDocId, false);
    AnalyzedOp newOp = op.andThenUpdateMod(colOp);

    andThenGeneric(colOp, newOp, nextExpectedType, onMatchCallback, onMismatchCallback);
  }

  protected void andThenUpdateSet(
      Consumer3<UpdateOplogOperation, KvDocument, KvDocument> onMatchCallback,
      BiConsumer<UpdateOplogOperation, KvDocument> onMismatchCallback) {

    AnalyzedOp op = getDefaultOp();
    AnalyzedOpType nextExpectedType = op.getType();

    UpdateOplogOperation colOp = createRandomUpdateSetOplogOperation(defaultMongoDocId, false);
    AnalyzedOp newOp = op.andThenUpdateMod(colOp);

    andThenGeneric(colOp, newOp, nextExpectedType, onMatchCallback, onMismatchCallback);
  }

  protected void andThenUpsertMod(
      Consumer3<UpdateOplogOperation, KvDocument, KvDocument> onMatchCallback,
      BiConsumer<UpdateOplogOperation, KvDocument> onMismatchCallback) {

    AnalyzedOp op = getDefaultOp();
    AnalyzedOpType nextExpectedType = op.getType();

    UpdateOplogOperation colOp = createRandomUpdateModOplogOperation(defaultMongoDocId, true);
    AnalyzedOp newOp = op.andThenUpsertMod(colOp);

    andThenGeneric(colOp, newOp, nextExpectedType, onMatchCallback, onMismatchCallback);
  }

  protected void andThenUpsertSet(
      Consumer3<UpdateOplogOperation, KvDocument, KvDocument> onMatchCallback,
      BiConsumer<UpdateOplogOperation, KvDocument> onMismatchCallback) {

    AnalyzedOp op = getDefaultOp();
    AnalyzedOpType nextExpectedType = op.getType();

    UpdateOplogOperation colOp = createRandomUpdateSetOplogOperation(defaultMongoDocId, true);
    AnalyzedOp newOp = op.andThenUpsertSet(colOp);

    andThenGeneric(colOp, newOp, nextExpectedType, onMatchCallback, onMismatchCallback);
  }

  protected void andThenDelete(
      Consumer3<DeleteOplogOperation, KvDocument, KvDocument> onMatchCallback,
      BiConsumer<DeleteOplogOperation, KvDocument> onMismatchCallback) {

    AnalyzedOp op = getDefaultOp();
    AnalyzedOpType nextExpectedType = op.getType();

    DeleteOplogOperation colOp = createRandomDeleteOplogOperation(defaultMongoDocId);
    AnalyzedOp newOp = op.andThenDelete(colOp);

    andThenGeneric(colOp, newOp, nextExpectedType, onMatchCallback, onMismatchCallback);
  }

  public KvDocument createDefaultKVDoc(KvValue<?> mongoDocId) {
    return new KvDocument.Builder()
        .putValue("_id", mongoDocId)
        .putValue("intAtt", KvInteger.of(RANDOM.nextInt()))
        .putValue("stringAtt", new StringKvString("a string value"))
        .build();
  }

  //Functions to be called by this class utility methods
  private <Op extends CollectionOplogOperation> void andThenGeneric(
      Op colOp, AnalyzedOp newOp, AnalyzedOpType nextExpectedType,
      Consumer3<Op, KvDocument, KvDocument> onMatchCallback,
      BiConsumer<Op, KvDocument> onMismatchCallback) {
    KvDocument matchDoc = createDefaultKVDoc(defaultMongoDocId);

    assertEquals("Unexpected type on the resulting operation after applying a a change",
        nextExpectedType, newOp.getType());
    testCalculateDocToInsert(matchDoc, newOp);

    onMatchCallback.accept(colOp, newOp.calculateDocToInsert(ignored -> matchDoc), matchDoc);
    onMismatchCallback.accept(colOp, newOp.calculateDocToInsert((ignored -> null)));
  }

  private UpdateOplogOperation createRandomUpdateModOplogOperation(KvValue<?> mongoDocId,
      boolean upsert) {
    return new UpdateOplogOperation(
        newDocument("_id", MongoWpConverter.translate(mongoDocId)),
        "db",
        "col",
        opTimeFactory.newOpTime(),
        RANDOM.nextLong(),
        OplogVersion.V1,
        false,
        newDocument("$set", newDocument("modifiedAtt", newInt(RANDOM.nextInt()))),
        upsert
    );
  }

  private UpdateOplogOperation createRandomUpdateSetOplogOperation(KvValue<?> mongoDocId,
      boolean upsert) {
    return new UpdateOplogOperation(
        newDocument("_id", MongoWpConverter.translate(mongoDocId)),
        "db",
        "col",
        opTimeFactory.newOpTime(),
        RANDOM.nextLong(),
        OplogVersion.V1,
        false,
        newDocument("modifiedAtt", newInt(RANDOM.nextInt())),
        upsert
    );
  }

  private DeleteOplogOperation createRandomDeleteOplogOperation(KvValue<?> mongoDocId) {
    return new DeleteOplogOperation(
        newDocument("_id", MongoWpConverter.translate(mongoDocId)),
        "db",
        "col",
        opTimeFactory.newOpTime(),
        RANDOM.nextLong(),
        OplogVersion.V1,
        false,
        false
    );
  }

  private void testCalculateDocToInsert(KvDocument matchDoc, AnalyzedOp newOp) {
    try {
      KvDocument docToInsert = newOp.calculateDocToInsert(otherOp -> matchDoc);
      //ops that requires fetch must return nonnull
      if (newOp.getType().requiresFetch()) {
        assertNotNull("The document to insert must not be null when the operation requires "
            + "to fetch ", docToInsert);
      }
      switch (newOp.getType()) {
        case DELETE_CREATE:
        case UPDATE_MOD:
        case UPDATE_SET:
        case UPSERT_MOD:
          assertNotNull("The document to insert must not when the operation type is "
              + newOp.getType() + " and the doc was fetch", docToInsert);
          break;
        default:
      }
    } catch (NullPointerException | IllegalArgumentException ex) {
      throw new AssertionError("Unexpected exception on a operation that does not "
          + "requires to fetch", ex);
    }

    //ops that does not require fetch must be ready to recive a function that returns null
    if (!newOp.getType().requiresFetch()) {
      try {
        KvDocument docToInsert = newOp.calculateDocToInsert(otherOp -> null);

        switch (newOp.getType()) {
          case DELETE_CREATE:
          case UPSERT_MOD:
            assertNotNull("The document to insert must not when the operation type is "
                + newOp.getType() + " even when the doc is not fetch", docToInsert);
            break;
          default:
        }

      } catch (NullPointerException | IllegalArgumentException ex) {
        throw new AssertionError("Unexpected exception on a operation that does not "
            + "requires to fetch", ex);
      }
    }
  }

}
