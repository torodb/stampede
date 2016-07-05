
package com.torodb.mongodb.commands.impl.general;

import javax.inject.Singleton;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteStatement;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.base.Splitter;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Builder;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.torod.WriteTorodTransaction;

/**
 *
 */
@Singleton
public class DeleteImplementation extends WriteTorodbCommandImpl<DeleteArgument, Long> {

    @Override
    public Status<Long> apply(Request req, Command<? super DeleteArgument, ? super Long> command, DeleteArgument arg,
            WriteMongodTransaction context) {
        Long deleted = 0l;

        for (DeleteStatement deleteStatement : arg.getStatements()) {
            BsonDocument query = deleteStatement.getQuery();
    
            switch (query.size()) {
                case 0: {
                    deleted += context.getTorodTransaction().deleteAll(req.getDatabase(), arg.getCollection());
                    break;
                }
                case 1: {
                    try {
                        deleted += deleteByAttribute(context.getTorodTransaction(), req.getDatabase(), arg.getCollection(), query);
                    } catch (CommandFailed ex) {
                        return Status.from(ex);
                    }
                    break;
                }
                default: {
                    return Status.from(ErrorCode.COMMAND_FAILED, "The given query is not supported right now");
                }
            }
        }

        return Status.ok(deleted);

    }

    private long deleteByAttribute(WriteTorodTransaction transaction, String db, String col, BsonDocument query) throws CommandFailed {
        Builder refBuilder = new AttributeReference.Builder();
        KVValue<?> kvValue = calculateValueAndAttRef(query, refBuilder);

        return transaction.deleteByAttRef(db, col, refBuilder.build(), kvValue);
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
