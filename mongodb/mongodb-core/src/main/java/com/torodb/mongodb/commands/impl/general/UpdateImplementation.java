
package com.torodb.mongodb.commands.impl.general;

import java.util.List;
import java.util.stream.Stream;

import javax.inject.Singleton;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateStatement;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.base.Splitter;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.document.UpdatedToroDocument;
import com.torodb.core.exceptions.UserWrappedException;
import com.torodb.core.exceptions.user.UpdateException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Builder;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.UpdateActionTranslator;
import com.torodb.mongodb.language.update.UpdateAction;
import com.torodb.mongodb.language.update.UpdatedToroDocumentBuilder;
import com.torodb.torod.WriteTorodTransaction;

/**
 *
 */
@Singleton
public class UpdateImplementation extends WriteTorodbCommandImpl<UpdateArgument, UpdateResult> {

    @Override
    public Status<UpdateResult> apply(Request req, Command<? super UpdateArgument, ? super UpdateResult> command, UpdateArgument arg,
            WriteMongodTransaction context) {
        long created = 0l;
        long candidatesSize = 0l;

        try {
            for (UpdateStatement updateStatement : arg.getStatements()) {
                BsonDocument query = updateStatement.getQuery();
                UpdateAction updateAction = UpdateActionTranslator.translate(updateStatement.getUpdate());
                Cursor<ToroDocument> candidatesCursor;
        
                switch (query.size()) {
                    case 0: {
                        candidatesCursor = context.getTorodTransaction().findAll(req.getDatabase(), arg.getCollection());
                        break;
                    }
                    case 1: {
                        try {
                            candidatesCursor = findByAttribute(context.getTorodTransaction(), req.getDatabase(), arg.getCollection(), query);
                        } catch (CommandFailed ex) {
                            return Status.from(ex);
                        }
                        break;
                    }
                    default: {
                        return Status.from(ErrorCode.COMMAND_FAILED, "The given query is not supported right now");
                    }
                }
                
                List<ToroDocument> candidates = candidatesCursor.getRemaining();
                candidatesSize += candidates.size();
                
                try {
                    Stream<KVDocument> updatedCandidates = candidates.stream()
                            .map(candidate -> {
                                try {
                                    return update(updateAction, candidate);
                                } catch(UserException userException) {
                                    throw new UserWrappedException(userException);
                                }
                            })
                            .map(updated -> updated.getRoot());
                    context.getTorodTransaction().insert(req.getDatabase(), arg.getCollection(), updatedCandidates);
                } catch(UserWrappedException userWrappedException) {
                    throw userWrappedException.getCause();
                }
            }
        } catch (UserException ex) {
            //TODO: Improve error reporting
            return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
        }

        return Status.ok(new UpdateResult(candidatesSize - created, candidatesSize));
    }
    
    protected UpdatedToroDocument update(UpdateAction updateAction, ToroDocument candidate) throws UpdateException {
        UpdatedToroDocumentBuilder builder = 
                UpdatedToroDocumentBuilder.from(candidate);
        updateAction.apply(builder);
        return builder.build();
    }

    private Cursor<ToroDocument> findByAttribute(WriteTorodTransaction transaction, String db, String col, BsonDocument query) throws CommandFailed, UserException {
        Builder refBuilder = new AttributeReference.Builder();
        KVValue<?> kvValue = calculateValueAndAttRef(query, refBuilder);

        return transaction.findByAttRef(db, col, refBuilder.build(), kvValue);
    }

    private KVValue<?> calculateValueAndAttRef(BsonDocument doc, AttributeReference.Builder refBuilder) throws CommandFailed {
        if (doc.size() != 1) {
            throw new CommandFailed("find", "The given query is not supported right now");
        }
        Entry<?> entry = doc.getFirstEntry();

        for (String subKey : Splitter.on('.').split(entry.getKey())) {
            refBuilder.addObjectKey(subKey);
        }

        if (entry.getValue().isArray()) {
            throw new CommandFailed("find", "Filters with arrays are not supported right now");
        }
        if (entry.getValue().isDocument()) {
            return calculateValueAndAttRef(entry.getValue().asDocument(), refBuilder);
        }
        else {
            return MongoWPConverter.translate(entry.getValue());
        }
    }

}
