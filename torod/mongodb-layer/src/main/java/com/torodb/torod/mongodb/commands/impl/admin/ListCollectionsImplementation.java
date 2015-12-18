
package com.torodb.torod.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult.Entry;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CollectionOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.tools.CursorMarshaller.FirstBatchOnlyCursor;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.utils.DocValueQueryCriteriaEvaluator;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.translator.BsonToToroTranslatorFunction;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import com.torodb.torod.mongodb.utils.JsonToBson;
import com.torodb.torod.mongodb.utils.NamespaceUtil;
import java.util.EnumSet;
import java.util.List;
import javax.inject.Inject;
import javax.json.JsonObject;
import org.bson.BsonDocument;

/**
 *
 */
public class ListCollectionsImplementation extends
        AbstractToroCommandImplementation<ListCollectionsArgument, ListCollectionsResult> {

    
    private final QueryCriteriaTranslator queryCriteriaTranslator;
    private final Function<CollectionMetainfo, ListCollectionsResult.Entry> transformation
            = new ToListCollectionsReplyEntryFunction();

    @Inject
    public ListCollectionsImplementation(
            QueryCriteriaTranslator queryCriteriaTranslator) {
        this.queryCriteriaTranslator = queryCriteriaTranslator;
    }

    @Override
    public CommandResult<ListCollectionsResult> apply(
            Command<? super ListCollectionsArgument, ? super ListCollectionsResult> command,
            CommandRequest<ListCollectionsArgument> req)
            throws MongoException {

        try {
            ListCollectionsArgument arg = req.getCommandArgument();

            ToroConnection connection = getToroConnection(req);

            Predicate<ListCollectionsResult.Entry> entryPredicate;
            if (arg.getFilter() == null || arg.getFilter().isEmpty()) {
                entryPredicate = Predicates.alwaysTrue();
            }
            else {
                /*
                 * The used way to implement the filter is very ineficient
                 * because it creates a lot of objects of different types that
                 * contais the same information. TODO: Improve performance. For
                 * example, we can evaluate the query on the mongowp objects
                 */
                QueryCriteria filter = queryCriteriaTranslator.translate(arg.getFilter());
                Predicate<DocValue> docValuePredicate =
                        DocValueQueryCriteriaEvaluator.createPredicate(filter);
                entryPredicate = new EntryPredicate(docValuePredicate);
            }
            UserCursor<CollectionMetainfo> cursor
                    = connection.openCollectionsMetainfoCursor();
            
            ImmutableList<ListCollectionsResult.Entry> firstBatch = ImmutableList.copyOf(
                    Iterables.filter(
                            Iterables.transform(
                                    cursor.readAll(),
                                    transformation
                            ),
                            entryPredicate
                    )
            );

            MongoCursor<ListCollectionsResult.Entry> resultCursor = new FirstBatchOnlyCursor<Entry>(
                    0,
                    req.getDatabase(),
                    NamespaceUtil.LIST_COLLECTIONS_GET_MORE_COLLECTION,
                    firstBatch,
                    System.currentTimeMillis()
            );
            ListCollectionsResult result = new ListCollectionsResult(resultCursor);

            return new NonWriteCommandResult<ListCollectionsResult>(result);
        } catch (ClosedToroCursorException ex) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "The cursor that iterates over the collections has been "
                            + "suddenly closed "
            );
        }
    }

    private static class ToListCollectionsReplyEntryFunction implements Function<CollectionMetainfo, ListCollectionsResult.Entry> {

        @Override
        public ListCollectionsResult.Entry apply(CollectionMetainfo input) {
            if (input == null) {
                return null;
            }
            
            ListCollectionsResult.Entry result = new Entry(
                    input.getName(),
                    new MyCollectionOptions(input)
            );

            return result;
        }
    }

    private static class MyCollectionOptions extends CollectionOptions {

        private final CollectionMetainfo metainfo;

        public MyCollectionOptions(CollectionMetainfo metainfo) {
            this.metainfo = metainfo;
        }

        @Override
        public boolean isCapped() {
            return metainfo.isCapped();
        }

        @Override
        public long getCappedSize() {
            return metainfo.getMaxSize();
        }

        @Override
        public long getCappedMaxDocs() {
            return metainfo.getMaxElements();
        }

        @Override
        public Long getInitialNumExtents() {
            return 1l;
        }

        @Override
        public List<Long> getInitialExtentSizes() {
            return null;
        }

        @Override
        public AutoIndexMode getAutoIndexMode() {
            return AutoIndexMode.DEFAULT;
        }

        @Override
        public EnumSet<Flag> getFlags() {
            return EnumSet.noneOf(Flag.class);
        }

        @Override
        public BsonDocument getStorageEngine() {
            if (metainfo.getJson() == null) {
                return new BsonDocument();
            }
            
            Preconditions.checkState(metainfo.getJson() instanceof JsonObject,
                    "Expected a json object as extra info from collection "
                    + metainfo.getName() + " but a "
                    + metainfo.getJson().getClass() + " was found");
            return JsonToBson.transform((JsonObject) metainfo.getJson());
        }

        @Override
        public boolean isTemp() {
            return false;
        }

    }

    private static class EntryPredicate implements Predicate<ListCollectionsResult.Entry> {

        private final Predicate<DocValue> docValuePredicate;

        public EntryPredicate(Predicate<DocValue> docValuePredicate) {
            this.docValuePredicate = docValuePredicate;
        }

        @Override
        public boolean apply(ListCollectionsResult.Entry input) {
            Function<Entry, BsonDocument> transformer = ListCollectionsResult.Entry.FROM_ENTRY;

            BsonDocument bson = transformer.apply(input);
            ToroDocument toroDoc = BsonToToroTranslatorFunction.INSTANCE.apply(bson);
            return docValuePredicate.apply(toroDoc.getRoot());
        }
    }
}
