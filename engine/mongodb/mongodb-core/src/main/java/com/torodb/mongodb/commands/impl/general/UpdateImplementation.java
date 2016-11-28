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

package com.torodb.mongodb.commands.impl.general;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.collect.ImmutableList;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.UserWrappedException;
import com.torodb.core.exceptions.user.UpdateException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Builder;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvDocument.DocEntry;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateArgument;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateResult;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateStatement;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpsertResult;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.Constants;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.mongodb.language.UpdateActionTranslator;
import com.torodb.mongodb.language.update.SetDocumentUpdateAction;
import com.torodb.mongodb.language.update.UpdateAction;
import com.torodb.mongodb.language.update.UpdatedToroDocumentBuilder;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.SharedWriteTorodTransaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class UpdateImplementation implements WriteTorodbCommandImpl<UpdateArgument, UpdateResult> {

  private final ObjectIdFactory objectIdFactory;

  private MongodMetrics mongodMetrics;

  @Inject
  public UpdateImplementation(ObjectIdFactory objectIdFactory, MongodMetrics mongodMetrics) {
    this.mongodMetrics = mongodMetrics;
    this.objectIdFactory = objectIdFactory;
  }

  @Override
  public Status<UpdateResult> apply(Request req,
      Command<? super UpdateArgument, ? super UpdateResult> command, UpdateArgument arg,
      WriteMongodTransaction context) {
    UpdateStatus updateStatus = new UpdateStatus();

    try {
      if (!context.getTorodTransaction().existsCollection(req.getDatabase(), arg.getCollection())) {
        context.getTorodTransaction().createIndex(req.getDatabase(), arg.getCollection(),
            Constants.ID_INDEX,
            ImmutableList.<IndexFieldInfo>of(new IndexFieldInfo(new AttributeReference(Arrays
                .asList(new Key[]{new ObjectKey(Constants.ID)})), FieldIndexOrdering.ASC
                .isAscending())), true);
      }

      for (UpdateStatement updateStatement : arg.getStatements()) {
        BsonDocument query = updateStatement.getQuery();
        UpdateAction updateAction = UpdateActionTranslator.translate(updateStatement.getUpdate());
        Cursor<ToroDocument> candidatesCursor;
        switch (query.size()) {
          case 0: {
            candidatesCursor = context.getTorodTransaction()
                .findAll(req.getDatabase(), arg.getCollection())
                .asDocCursor();
            break;
          }
          case 1: {
            try {
              candidatesCursor = findByAttribute(context.getTorodTransaction(), req.getDatabase(),
                  arg.getCollection(), query);
            } catch (CommandFailed ex) {
              return Status.from(ex);
            }
            break;
          }
          default: {
            return Status.from(ErrorCode.COMMAND_FAILED,
                "The given query is not supported right now");
          }
        }

        if (candidatesCursor.hasNext()) {
          try {
            Stream<List<ToroDocument>> candidatesbatchStream;
            if (updateStatement.isMulti()) {
              candidatesbatchStream = StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(candidatesCursor.batch(100),
                      Spliterator.ORDERED), false);
            } else {
              candidatesbatchStream = Stream.of(ImmutableList.of(candidatesCursor.next()));
            }
            Stream<KvDocument> updatedCandidates = candidatesbatchStream
                .map(candidates -> {
                  updateStatus.increaseCandidates(candidates.size());
                  context.getTorodTransaction().delete(req.getDatabase(), arg.getCollection(),
                      candidates);
                  return candidates;
                })
                .flatMap(l -> l.stream())
                .map(candidate -> {
                  try {
                    updateStatus.increaseUpdated();
                    return update(updateAction, candidate);
                  } catch (UserException userException) {
                    throw new UserWrappedException(userException);
                  }
                });
            context.getTorodTransaction().insert(req.getDatabase(), arg.getCollection(),
                updatedCandidates);
          } catch (UserWrappedException userWrappedException) {
            throw userWrappedException.getCause();
          }
        } else if (updateStatement.isUpsert()) {
          KvDocument toInsertCandidate;
          if (updateAction instanceof SetDocumentUpdateAction) {
            toInsertCandidate = ((SetDocumentUpdateAction) updateAction).getNewValue();
          } else {
            toInsertCandidate =
                update(updateAction,
                    new ToroDocument(-1, (KvDocument) MongoWpConverter.translate(query)));
          }
          if (!toInsertCandidate.containsKey(Constants.ID)) {
            KvDocument.Builder builder = new KvDocument.Builder();
            for (DocEntry<?> entry : toInsertCandidate) {
              builder.putValue(entry.getKey(), entry.getValue());
            }
            builder.putValue(Constants.ID, MongoWpConverter.translate(objectIdFactory
                .consumeObjectId()));
            toInsertCandidate = builder.build();
          }
          updateStatus.increaseCandidates(1);
          updateStatus.increaseCreated(toInsertCandidate.get(Constants.ID));
          Stream<KvDocument> toInsertCandidates = Stream.of(toInsertCandidate);
          context.getTorodTransaction().insert(req.getDatabase(), arg.getCollection(),
              toInsertCandidates);
        }
      }
    } catch (UserException ex) {
      //TODO: Improve error reporting
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
    }
    mongodMetrics.getUpdateModified().mark(updateStatus.updated);
    mongodMetrics.getUpdateMatched().mark(updateStatus.candidates);
    mongodMetrics.getUpdateUpserted().mark(updateStatus.upsertResults.size());
    return Status.ok(new UpdateResult(updateStatus.updated, updateStatus.candidates,
        ImmutableList.copyOf(updateStatus.upsertResults)));
  }

  private static class UpdateStatus {

    int candidates = 0;
    int updated = 0;
    int created = 0;
    List<UpsertResult> upsertResults = new ArrayList<>();

    void increaseCandidates(int size) {
      candidates += size;
    }

    void increaseUpdated() {
      updated++;
    }

    void increaseCreated(KvValue<?> id) {
      upsertResults.add(new UpsertResult(created,
          MongoWpConverter.translate(id)));
      created++;
    }
  }

  protected KvDocument update(UpdateAction updateAction, ToroDocument candidate) throws
      UpdateException {
    UpdatedToroDocumentBuilder builder =
        UpdatedToroDocumentBuilder.from(candidate);
    updateAction.apply(builder);
    return builder.build();
  }

  private Cursor<ToroDocument> findByAttribute(SharedWriteTorodTransaction transaction, String db,
      String col, BsonDocument query) throws CommandFailed, UserException {
    Builder refBuilder = new AttributeReference.Builder();
    KvValue<?> kvValue = AttrRefHelper.calculateValueAndAttRef(query, refBuilder);

    return transaction.findByAttRef(db, col, refBuilder.build(), kvValue).asDocCursor();
  }

}
