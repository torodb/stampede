
package com.torodb.mongodb.commands.impl.general;

import java.util.Map;
import java.util.function.Function;

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
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Builder;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.torod.WriteTorodTransaction;

/**
 *
 */
@Singleton
public class UpdateImplementation extends WriteTorodbCommandImpl<UpdateArgument, UpdateResult> {

    @Override
    public Status<UpdateResult> apply(Request req, Command<? super UpdateArgument, ? super UpdateResult> command, UpdateArgument arg,
            WriteMongodTransaction context) {
        Long created = 0l;
        Long modified = 0l;

        for (UpdateStatement updateStatement : arg.getStatements()) {
            BsonDocument query = updateStatement.getQuery();
            BsonDocument update = updateStatement.getUpdate();
            //TODO: Update just replace the document contet with the exception of the "_id"
            Function<KVDocument, KVDocument> updateFunction = 
                    (d) -> {
                        KVDocument.Builder builder = new KVDocument.Builder();
                        
                        for (DocEntry<?> entry : MongoWPConverter.toEagerDocument(update)) {
                            builder.putValue(entry.getKey(), entry.getValue());
                        }
                        
                        if (d.containsKey("_id")) {
                            builder.putValue("_id", d.get("_id"));
                        }
                        
                        return builder.build();
                    };
    
            try {
                switch (query.size()) {
                    case 0: {
                        com.torodb.core.backend.UpdateResult updateResult = context.getTorodTransaction().updateAll(req.getDatabase(), arg.getCollection(), updateFunction);
                        created += updateResult.getCreated();
                        modified += updateResult.getModified();
                        break;
                    }
                    case 1: {
                        try {
                            com.torodb.core.backend.UpdateResult updateResult = deleteByAttribute(context.getTorodTransaction(), req.getDatabase(), arg.getCollection(), query, updateFunction);
                            created += updateResult.getCreated();
                            modified += updateResult.getModified();
                        } catch (CommandFailed ex) {
                            return Status.from(ex);
                        }
                        break;
                    }
                    default: {
                        return Status.from(ErrorCode.COMMAND_FAILED, "The given query is not supported right now");
                    }
                }
            } catch (UserException ex) {
                //TODO: Improve error reporting
                return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
            }
        }

        return Status.ok(new UpdateResult(modified, modified));

    }

    private com.torodb.core.backend.UpdateResult deleteByAttribute(WriteTorodTransaction transaction, String db, String col, BsonDocument query, Function<KVDocument, KVDocument> updateFunction) throws CommandFailed, UserException {
        Builder refBuilder = new AttributeReference.Builder();
        KVValue<?> kvValue = calculateValueAndAttRef(query, refBuilder);

        return transaction.updateByAttRef(db, col, refBuilder.build(), kvValue, updateFunction);
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
