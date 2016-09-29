/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.mongodb.commands.impl.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions.IndexType;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.Constants;

public class CreateIndexesImplementation implements WriteTorodbCommandImpl<CreateIndexesArgument, CreateIndexesResult> {

    @Override
    public Status<CreateIndexesResult> apply(Request req, Command<? super CreateIndexesArgument, ? super CreateIndexesResult> command,
            CreateIndexesArgument arg, WriteMongodTransaction context) {
        boolean existsCollection = context.getTorodTransaction().existsCollection(req.getDatabase(), arg.getCollection());
        if (!existsCollection) {
            context.getTorodTransaction().createIndex(req.getDatabase(), arg.getCollection(), Constants.ID_INDEX, 
                    new AttributeReference(Arrays.asList(new Key[] { new ObjectKey(Constants.ID) })), FieldIndexOrdering.ASC, true);
        }
        
        int indexesBefore = (int) context.getTorodTransaction().getIndexesInfo(req.getDatabase(), arg.getCollection()).count();
        int indexesAfter = indexesBefore;
        
        try {
            boolean createdCollectionAutomatically = !existsCollection;
            
            for (IndexOptions indexOptions : arg.getIndexesToCreate()) {
                if (indexOptions.getKeys().size() < 1) {
                    throw new CommandFailed("createIndexes", 
                            "Index must be on at least on one field");
                }
                if (indexOptions.getKeys().size() > 1) {
                    throw new CommandFailed("createIndexes", 
                            "Index on more than one field is not supported right now");
                }

                AttributeReference.Builder refBuilder = new AttributeReference.Builder();
                Map.Entry<List<String>, IndexType> indexEntry = 
                        indexOptions.getKeys().entrySet().iterator().next();
                for (String key : indexEntry.getKey()) {
                    refBuilder.addObjectKey(key);
                }
                
                FieldIndexOrdering ordering;
                switch(indexEntry.getValue()) {
                case asc:
                    ordering = FieldIndexOrdering.ASC;
                    break;
                case desc:
                    ordering = FieldIndexOrdering.DESC;
                    break;
                case geospatial:
                case hashed:
                case text:
                default:
                    throw new CommandFailed("createIndexes", 
                            "Index of type " + indexEntry.getValue().name() + " is not suppoeted right now");
                }
                
                if (indexOptions.isBackground()) {
                    throw new CommandFailed("createIndexes", 
                            "Building index in background is not suppoeted right now");
                }
                
                if (indexOptions.isSparse()) {
                    throw new CommandFailed("createIndexes", 
                            "Sparse index are not suppoeted right now");
                }
                
                context.getTorodTransaction().createIndex(req.getDatabase(), arg.getCollection(), indexOptions.getName(), refBuilder.build(), ordering, indexOptions.isUnique());
                
                indexesAfter++;
            }

            return Status.ok(new CreateIndexesResult(indexesBefore, indexesAfter, null, createdCollectionAutomatically));
        } catch(CommandFailed ex) {
            return Status.from(ex);
        }
    }

}
