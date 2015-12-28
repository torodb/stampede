
package com.torodb.torod.mongodb.commands.impl.torodb;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.tools.CursorMarshaller.FirstBatchOnlyCursor;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.InternalErrorException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.ValueRow.ForEachConsumer;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.commands.torodb.SqlSelectCommand.SqlSelectArgument;
import com.torodb.torod.mongodb.commands.torodb.SqlSelectCommand.SqlSelectResult;
import com.torodb.torod.mongodb.utils.ToroDBThrowables;
import java.util.Iterator;
import javax.annotation.Nonnull;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 *
 */
public class SqlSelectImplementation extends
        AbstractToroCommandImplementation<SqlSelectArgument, SqlSelectResult> {

    private static final ValueRowToBsonDocument TRANSLATE_FUNCTION = new ValueRowToBsonDocument();

    @Override
    public CommandResult<SqlSelectResult> apply(
            Command<? super SqlSelectArgument, ? super SqlSelectResult> command,
            CommandRequest<SqlSelectArgument> req) throws MongoException {

        RequestContext context = RequestContext.getFrom(req);
        String supportedDatabase = context.getSupportedDatabase();

        String commandName = command.getCommandName();
        SqlSelectArgument arg = req.getCommandArgument();

        if (!supportedDatabase.equals(req.getDatabase())) {
            throw new CommandFailed(
                    commandName,
                    "Database '"+req.getDatabase()+"' is not supported. "
                            + "Only '" + supportedDatabase +"' is supported");
        }

        ToroConnection connection = context.getToroConnection();
        ToroTransaction transaction = null;

        try {
            transaction = connection.createTransaction();
            Iterator<ValueRow<DocValue>> toroQueryResult = ToroDBThrowables.getFromCommand(
                    commandName,
                    transaction.sqlSelect(arg.getQuery())
            );
            ToroDBThrowables.getFromCommand(commandName, transaction.commit());

            ImmutableList<BsonDocument> queryResult = ImmutableList.copyOf(
                    Iterators.transform(
                            toroQueryResult,
                            TRANSLATE_FUNCTION
                    )
            );

            MongoCursor<BsonDocument> cursor = new FirstBatchOnlyCursor<>(
                    0,
                    req.getDatabase(),
                    arg.getCollection(),
                    queryResult,
                    System.currentTimeMillis()
            );

            return new NonWriteCommandResult<>(new SqlSelectResult(cursor));
        } catch (ImplementationDbException ex) {
            throw new InternalErrorException(command.getCommandName(), ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

    }

    private static class ValueRowToBsonDocument implements 
            Function<ValueRow<DocValue>, BsonDocument> {

        @Override
        public BsonDocument apply(@Nonnull ValueRow<DocValue> input) {

            Collector collector = new Collector();

            input.consume(collector);
            return collector.doc;
        }

        private static class Collector implements ForEachConsumer<DocValue> {

            private final BsonDocument doc = new BsonDocument();

            @Override
            public void consume(String key, DocValue value) {
                BsonValue translated = MongoValueConverter.translateDocValue(value);
                doc.append(key, translated);
            }
        }

    }

}
