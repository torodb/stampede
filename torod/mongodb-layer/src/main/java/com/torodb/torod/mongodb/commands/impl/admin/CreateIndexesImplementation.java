/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with mongodb-layer. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.torod.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.DatabaseNotFoundException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.Sets;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.AttributeReference.Builder;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;

/**
 *
 */
public class CreateIndexesImplementation extends AbstractToroCommandImplementation<CreateIndexesArgument, CreateIndexesResult>{

    private static final Set<String> SUPPORTED_FIELDS = Sets.newHashSet("name", "key", "unique", "sparse", "ns");

    private final String supporteDatabase;

    @Inject
    public CreateIndexesImplementation(@DatabaseName String supporteDatabase) {
        this.supporteDatabase = supporteDatabase;
    }

    @Override
    public CommandResult<CreateIndexesResult> apply(
            Command<? super CreateIndexesArgument, ? super CreateIndexesResult> command,
            CommandRequest<CreateIndexesArgument> req) throws MongoException {

        CreateIndexesArgument arg = req.getCommandArgument();
        String collection = arg.getCollection();
        ToroConnection connection = getToroConnection(req);


        int numIndexesBefore;

        try (ToroTransaction transaction = connection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {

            numIndexesBefore = transaction.getIndexes(collection).size();

            for (IndexOptions indexOptions : arg.getIndexesToCreate()) {
                String name = indexOptions.getName();
                Map<List<String>, Boolean> key = indexOptions.getKeys();
                boolean unique = indexOptions.isUnique();
                boolean sparse = indexOptions.isSparse();

                checkNamespace(indexOptions.getDatabase(), indexOptions.getCollection(), supporteDatabase, collection);

                Set<String> extraOptions = new HashSet<>();
                for (Entry<?> otherProp : indexOptions.getOtherProps()) {
                    String option = otherProp.getKey();
                    if (!SUPPORTED_FIELDS.contains(option)) {
                        extraOptions.add(option);
                    }
                }
                if (!extraOptions.isEmpty()) {
                    boolean safeExtraOptions = true;
                    for (String extraOption : extraOptions) {
                        if (!extraOption.equals("background")) {
                            safeExtraOptions = false;
                            break;
                        }
                    }
                    if (safeExtraOptions) {
                        Entry<?> backgroundEntry
                                = indexOptions.getOtherProps().getEntry("background");
                        if (backgroundEntry != null) {
                            safeExtraOptions
                                    = !BsonReaderTool.getBooleanOrNumeric(backgroundEntry, "background");
                        }
                    }

                    if (!safeExtraOptions) {
                        throw new CommandFailed(
                                CreateIndexesCommand.INSTANCE.getCommandName(),
                                "Options " + extraOptions + " are not supported"
                        );
                    }
                }

                IndexedAttributes.Builder indexedAttsBuilder
                        = new IndexedAttributes.Builder();

                for (java.util.Map.Entry<List<String>, Boolean> entry : key.entrySet()) {
                    AttributeReference attRef = parseAttributeReference(entry.getKey());

                    indexedAttsBuilder.addAttribute(attRef, entry.getValue());
                }

                transaction.createIndex(collection, name, indexedAttsBuilder.build(), unique, sparse).get();
            }

            int numIndexesAfter = transaction.getIndexes(collection).size();

            transaction.commit().get();

            return new NonWriteCommandResult<>(
                    new CreateIndexesResult(numIndexesBefore, numIndexesAfter, null, false)
            );
        } catch (ImplementationDbException | ExecutionException | InterruptedException ex) {
            throw new UnknownErrorException(ex);
        }
    }

    private void checkNamespace(String db, String col, String actualDb, String actualCol) throws DatabaseNotFoundException, CommandFailed {
        if (db == null || col == null) {
            assert db == null && col == null;
            return ;
        }

        if (!db.equals(supporteDatabase)) {
            throw new DatabaseNotFoundException(db);
        }
        if (!db.equals(actualDb) || !col.equals(actualCol)) {
            throw new CommandFailed(CreateIndexesCommand.INSTANCE.getCommandName(), "namespace mismatch");
        }
    }

    private AttributeReference parseAttributeReference(List<String> key) {
        AttributeReference.Builder builder = new Builder();
        for (String navigation : key) {
            builder.addObjectKey(navigation);
        }
        return builder.build();
    }

}
