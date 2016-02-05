
package com.torodb.torod.mongodb.commands.impl.torodb;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.InternalErrorException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.tools.CursorMarshaller.FirstBatchOnlyCursor;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.ValueRow.ForEachConsumer;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.commands.torodb.SqlSelectCommand.SqlSelectArgument;
import com.torodb.torod.mongodb.commands.torodb.SqlSelectCommand.SqlSelectResult;
import com.torodb.torod.mongodb.utils.ToroDBThrowables;
import java.util.Iterator;
import javax.annotation.Nonnull;

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

        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.READ_ONLY)) {
            Iterator<ValueRow<KVValue<?>>> toroQueryResult = ToroDBThrowables.getFromCommand(
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
        }

    }

    private static class ValueRowToBsonDocument implements 
            Function<ValueRow<KVValue<?>>, BsonDocument> {

        @Override
        public BsonDocument apply(@Nonnull ValueRow<KVValue<?>> input) {

            Collector collector = new Collector();

            input.consume(collector);
            return collector.doc.build();
        }

        private static class Collector implements ForEachConsumer<KVValue<?>> {

            private final BsonDocumentBuilder doc = new BsonDocumentBuilder();

            @Override
            public void consume(String key, KVValue<?> value) {
                BsonValue translated = MongoWPConverter.translate(value);
                doc.appendUnsafe(key, translated);
            }
        }

    }

}
