
package com.torodb.torod.mongodb.commands.torodb;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.*;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.impl.AbstractCommand;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.annotations.Beta;

/**
 * A backend dependient command that creates easy to use SQL views on the
 * backend database.
 */
@Beta
public class CreatePathViewsCommand extends AbstractCommand<CollectionCommandArgument, Integer> {

    private static final IntField RESULT_FIELD = new IntField("viewsCounter");
    private static final StringField BETA_FIELD = new StringField("betaCmd");
    public static final CreatePathViewsCommand INSTANCE = new CreatePathViewsCommand();

    private CreatePathViewsCommand() {
        super("createPathViews");
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
    public Class<? extends Integer> getResultClass() {
        return Integer.class;
    }

    @Override
    public Integer unmarshallResult(BsonDocument resultDoc) throws
            BadValueException, TypesMismatchException, NoSuchKeyException,
            FailedToParseException, MongoException {
        return BsonReaderTool.getInteger(resultDoc, RESULT_FIELD);
    }

    @Override
    public BsonDocument marshallResult(Integer result) throws MarshalException {
        return new BsonDocumentBuilder()
                .append(RESULT_FIELD, result)
                .append(BETA_FIELD, "This is a beta command")
                .build();
    }

}
