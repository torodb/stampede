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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions.IndexVersion;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.tools.CursorMarshaller.FirstBatchOnlyCursor;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.google.common.collect.ImmutableList;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.AttributeReference.Key;
import com.torodb.torod.core.pojos.IndexedAttributes.IndexType;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.utils.IndexTypeUtils;

/**
 *
 */
public class ListIndexesImplementation extends AbstractToroCommandImplementation<ListIndexesArgument, ListIndexesResult> {

    private final String databaseName;

    @Inject
    public ListIndexesImplementation(@DatabaseName String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public CommandResult<ListIndexesResult> apply(
            Command<? super ListIndexesArgument, ? super ListIndexesResult> command,
            CommandRequest<ListIndexesArgument> req) throws MongoException {

        ListIndexesArgument arg = req.getCommandArgument();
        String collection = arg.getCollection();
        ToroConnection connection = getToroConnection(req);

        Collection<? extends NamedToroIndex> indexes;
        try (ToroTransaction transaction = connection.createTransaction(TransactionMetainfo.READ_ONLY)) {
            indexes = transaction.getIndexes(collection);
        } catch (ImplementationDbException ex) {
            throw new UnknownErrorException(ex);
        }

        List<IndexOptions> indexOptions = new ArrayList<>(indexes.size());

        for (NamedToroIndex index : indexes) {
            indexOptions.add(translate(index));
        }

        return new NonWriteCommandResult<>(new ListIndexesResult(new FirstBatchOnlyCursor<>(
                0,
                databaseName,
                collection,
                ImmutableList.copyOf(indexOptions),
                System.currentTimeMillis()
        )));
    }

    private IndexOptions translate(NamedToroIndex index) {
        Map<List<String>, IndexOptions.IndexType> keysMap = new HashMap<>(index.getAttributes().size());
        for (java.util.Map.Entry<AttributeReference, IndexType> entry : index.getAttributes().entrySet()) {
            ArrayList<String> keyPath = new ArrayList<>(entry.getKey().getKeys().size());
            for (Key key : entry.getKey().getKeys()) {
                keyPath.add(key.getKeyValue().toString());
            }
            keysMap.put(keyPath, IndexTypeUtils.fromIndexType(entry.getValue()));
        }

        return new IndexOptions(
                IndexVersion.V2,
                index.getName(),
                databaseName,
                index.getCollection(),
                false,
                false,
                false,
                0,
                keysMap,
                null,
                null
        );
    }

}
