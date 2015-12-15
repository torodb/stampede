
package com.torodb.torod.mongodb.commands.torodb;

import com.eightkdata.mongowp.mongoserver.api.safe.MarshalException;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.AbstractCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.Empty;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonDocumentBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonField;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.*;
import com.google.common.annotations.Beta;
import org.bson.BsonDocument;

/**
 *
 */
@Beta
public class DropPathViewsCommand extends AbstractCommand<CollectionCommandArgument, Empty> {

    private static final BsonField<String> BETA_FIELD = BsonField.create("betaCmd");
    public static final DropPathViewsCommand INSTANCE = new DropPathViewsCommand();

    private DropPathViewsCommand() {
        super("dropPathViews");
    }

    @Override
    public Class<? extends CollectionCommandArgument> getArgClass() {
        return CollectionCommandArgument.class;
    }

    @Override
    public CollectionCommandArgument unmarshallArg(BsonDocument requestDoc) throws
            BadValueException, TypesMismatchException, NoSuchKeyException,
            FailedToParseException {
        return CollectionCommandArgument.unmarshall(requestDoc, INSTANCE);
    }

    @Override
    public BsonDocument marshallArg(CollectionCommandArgument request) throws MarshalException {
        return request.marshall();
    }

    @Override
    public Class<? extends Empty> getResultClass() {
        return Empty.class;
    }

    @Override
    public Empty unmarshallResult(BsonDocument resultDoc) throws
            BadValueException, TypesMismatchException, NoSuchKeyException,
            FailedToParseException, MongoException {
        return Empty.getInstance();
    }

    @Override
    public BsonDocument marshallResult(Empty result) throws MarshalException {
        return new BsonDocumentBuilder()
                .append(BETA_FIELD, "This is a beta command")
                .build();
    }

}