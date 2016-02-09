
package com.torodb.torod.mongodb.commands.torodb;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.*;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.impl.AbstractCommand;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.google.common.annotations.Beta;

/**
 *
 */
@Beta
public class DropPathViewsCommand extends AbstractCommand<CollectionCommandArgument, Empty> {

    private static final StringField BETA_FIELD = new StringField("betaCmd");
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