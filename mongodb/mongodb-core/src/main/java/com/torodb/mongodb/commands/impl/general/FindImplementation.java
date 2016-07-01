
package com.torodb.mongodb.commands.impl.general;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand.FindArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand.FindResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CursorResult;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.base.Splitter;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Builder;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.conversion.mongowp.ToBsonDocumentTranslator;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.core.MongodTransaction;
import com.torodb.torod.TorodTransaction;
import java.util.List;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class FindImplementation extends ReadTorodbCommandImpl<FindArgument, FindResult> {

    @Override
    public Status<FindResult> apply(Request req, Command<? super FindArgument, ? super FindResult> command, FindArgument arg, MongodTransaction context) {
        BsonDocument filter = arg.getFilter();

        Cursor<BsonDocument> cursor;

        switch (filter.size()) {
            case 0: {
                cursor = context.getTorodTransaction().findAll(req.getDatabase(), arg.getCollection())
                        .transform(ToBsonDocumentTranslator.getInstance());
                break;
            }
            case 1: {
                try {
                    cursor = getByAttributeCursor(context.getTorodTransaction(), req.getDatabase(), arg.getCollection(), filter)
                            .transform(ToBsonDocumentTranslator.getInstance());
                } catch (CommandFailed ex) {
                    return Status.from(ex);
                }
                break;
            }
            default: {
                return Status.from(ErrorCode.COMMAND_FAILED, "The given query is not supported right now");
            }
        }

        List<BsonDocument> batch = cursor.getNextBatch(100);
        cursor.close();

        return Status.ok(new FindResult(CursorResult.createSingleBatchCursor(req.getDatabase(), arg.getCollection(), batch.iterator())));

    }

    private Cursor<KVDocument> getByAttributeCursor(TorodTransaction transaction, String db, String col, BsonDocument filter) throws CommandFailed {

        Builder refBuilder = new AttributeReference.Builder();
        KVValue<?> kvValue = calculateValueAndAttRef(filter, refBuilder);

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
