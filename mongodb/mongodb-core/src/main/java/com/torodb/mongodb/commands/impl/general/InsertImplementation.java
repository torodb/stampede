
package com.torodb.mongodb.commands.impl.general;

import java.util.Arrays;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.kvdocument.conversion.mongowp.FromBsonValueTranslator;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.Constants;

/**
 *
 */
@Singleton
public class InsertImplementation implements WriteTorodbCommandImpl<InsertArgument, InsertResult>{
	
	private MongodMetrics mongodMetrics;
	
	@Inject
	public InsertImplementation(MongodMetrics mongodMetrics) {
		this.mongodMetrics = mongodMetrics;
	}

    @Override
    public Status<InsertResult> apply(Request req, Command<? super InsertArgument, ? super InsertResult> command, InsertArgument arg, WriteMongodTransaction context) {
        if (!context.getTorodTransaction().existsCollection(req.getDatabase(), arg.getCollection())) {
            context.getTorodTransaction().createIndex(req.getDatabase(), arg.getCollection(), Constants.ID_INDEX, 
                    new AttributeReference(Arrays.asList(new Key[] { new ObjectKey(Constants.ID) })), FieldIndexOrdering.ASC, true);
        }
        
    	mongodMetrics.getInserts().mark(arg.getDocuments().size());
    	Stream<KVDocument> docsToInsert = arg.getDocuments().stream().map(FromBsonValueTranslator.getInstance())
                .map((v) -> (KVDocument)v);

        try {
            context.getTorodTransaction().insert(req.getDatabase(), arg.getCollection(), docsToInsert);
        } catch (UserException ex) {
            //TODO: Improve error reporting
            return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
        }

        return Status.ok(new InsertResult(arg.getDocuments().size()));
    }
}
