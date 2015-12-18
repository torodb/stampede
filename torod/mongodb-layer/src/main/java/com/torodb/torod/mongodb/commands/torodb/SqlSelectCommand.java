
package com.torodb.torod.mongodb.commands.torodb;

import com.eightkdata.mongowp.mongoserver.api.safe.MarshalException;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.AbstractCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.tools.CursorMarshaller;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonDocumentBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonField;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonReaderTool;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.*;
import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.torodb.torod.mongodb.commands.torodb.SqlSelectCommand.SqlSelectArgument;
import com.torodb.torod.mongodb.commands.torodb.SqlSelectCommand.SqlSelectResult;
import javax.annotation.Nonnull;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 *
 */
@Beta
public class SqlSelectCommand extends AbstractCommand<SqlSelectArgument, SqlSelectResult> {

    private static final String COMMAND_NAME = "sql-select";
    public static final SqlSelectCommand INSTANCE = new SqlSelectCommand();

    private SqlSelectCommand() {
        super(COMMAND_NAME);
    }

    @Override
    public Class<? extends SqlSelectArgument> getArgClass() {
        return SqlSelectArgument.class;
    }

    @Override
    public SqlSelectArgument unmarshallArg(BsonDocument requestDoc)
            throws BadValueException, TypesMismatchException, NoSuchKeyException,
            FailedToParseException {
        return SqlSelectArgument.unmarshall(requestDoc);
    }

    @Override
    public BsonDocument marshallArg(SqlSelectArgument request) throws
            MarshalException {
        return request.marshall();
    }

    @Override
    public Class<? extends SqlSelectResult> getResultClass() {
        return SqlSelectResult.class;
    }

    @Override
    public SqlSelectResult unmarshallResult(BsonDocument resultDoc) throws
            BadValueException, TypesMismatchException, NoSuchKeyException,
            FailedToParseException, MongoException {
        return SqlSelectResult.unmarshall(resultDoc);
    }

    @Override
    public BsonDocument marshallResult(SqlSelectResult result) throws MarshalException {
        return result.marshall();
    }

    public static class SqlSelectArgument {

        private static final BsonField<String> COLLECTION_FIELD = BsonField.create(COMMAND_NAME);
        private static final BsonField<String> QUERY_FIELD = BsonField.create("query");

        private final String collection;
        private final String query;

        public SqlSelectArgument(String collection, String query) {
            this.collection = collection;
            this.query = query;
        }

        public String getCollection() {
            return collection;
        }

        public String getQuery() {
            return query;
        }

        private static SqlSelectArgument unmarshall(BsonDocument requestDoc) 
                throws TypesMismatchException, NoSuchKeyException {
            String collection = BsonReaderTool.getString(requestDoc, COLLECTION_FIELD);
            String query = BsonReaderTool.getString(requestDoc, QUERY_FIELD);
            return new SqlSelectArgument(collection, query);
        }

        private BsonDocument marshall() {
            return new BsonDocumentBuilder()
                    .append(COLLECTION_FIELD, collection)
                    .append(QUERY_FIELD, query)
                    .build();
        }
        
    }

    public static class SqlSelectResult {

        private static final BsonField<BsonDocument> CURSOR_FIELD = BsonField.create("cursor");
        private static final ToBsonFunction TO_BSON = new ToBsonFunction();
        private static final FromBsonFunction FROM_BSON = new FromBsonFunction();
        private static final BsonField<String> BETA_FIELD = BsonField.create("betaCmd");

        private final MongoCursor<BsonDocument> cursor;

        public SqlSelectResult(MongoCursor<BsonDocument> cursor) {
            this.cursor = cursor;
        }

        public MongoCursor<BsonDocument> getCursor() {
            return cursor;
        }

        private static SqlSelectResult unmarshall(BsonDocument resultDoc)
                throws TypesMismatchException, NoSuchKeyException, BadValueException {
            BsonDocument cursorDoc = BsonReaderTool.getDocument(resultDoc, CURSOR_FIELD);

            return new SqlSelectResult(
                    CursorMarshaller.unmarshall(cursorDoc, TO_BSON)
            );
        }

        private BsonDocument marshall() throws MarshalException {
            BsonDocumentBuilder builder = new BsonDocumentBuilder();

            try {
                return builder
                        .append(CURSOR_FIELD, CursorMarshaller.marshall(cursor, FROM_BSON))
                        .append(BETA_FIELD, "This is a beta command")
                        .build();
            } catch (MongoException ex) {
                throw new MarshalException(ex);
            }
        }

        private static class ToBsonFunction implements Function<BsonValue, BsonDocument> {

            @Override
            public BsonDocument apply(@Nonnull BsonValue input) {
                if (input instanceof BsonDocument) {
                    return (BsonDocument) input;
                }
                throw new IllegalArgumentException("");
            }

        }

        private static class FromBsonFunction implements Function<BsonDocument, BsonValue> {

            @Override
            public BsonValue apply(BsonDocument input) {
                return input;
            }

        }

    }

}
