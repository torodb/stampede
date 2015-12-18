
package com.torodb.torod.mongodb.commands.torodb;

import com.eightkdata.mongowp.mongoserver.api.safe.MarshalException;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.AbstractCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonDocumentBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonField;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonReaderTool;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.*;
import com.google.common.annotations.Beta;
import org.bson.BsonDocument;

/**
 * A backend dependient command that creates easy to use SQL views on the
 * backend database.
 */
@Beta
public class CreatePathViewsCommand extends AbstractCommand<CollectionCommandArgument, Integer> {

    private static final BsonField<Integer> RESULT_FIELD = BsonField.create("viewsCounter");
    private static final BsonField<String> BETA_FIELD = BsonField.create("betaCmd");
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
