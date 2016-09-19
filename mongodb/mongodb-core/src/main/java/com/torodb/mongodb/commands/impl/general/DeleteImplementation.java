
package com.torodb.mongodb.commands.impl.general;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteStatement;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Builder;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.torod.SharedWriteTorodTransaction;

/**
 *
 */
@Singleton
public class DeleteImplementation implements WriteTorodbCommandImpl<DeleteArgument, Long> {

	private MongodMetrics mongodMetrics;
	
	@Inject
	public DeleteImplementation(MongodMetrics mongodMetrics) {
		this.mongodMetrics = mongodMetrics;
	}
	
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
        mongodMetrics.getDeletes().mark(deleted);
        return Status.ok(deleted);

    }

    private long deleteByAttribute(SharedWriteTorodTransaction transaction, String db, String col, BsonDocument query) throws CommandFailed {
        Builder refBuilder = new AttributeReference.Builder();
        KVValue<?> kvValue = AttrRefHelper.calculateValueAndAttRef(query, refBuilder);
        return transaction.deleteByAttRef(db, col, refBuilder.build(), kvValue);
    }

}
