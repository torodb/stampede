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
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.commands;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand.DropIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand.DropIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.index.IndexOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.index.IndexOptions.KnownType;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.language.AttributeReference;
import com.torodb.mongodb.language.Constants;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.SharedWriteTorodTransaction;

/**
 *
 */
public class DropIndexesReplImpl extends ReplCommandImpl<DropIndexesArgument, DropIndexesResult> {
    private static final Logger LOGGER
            = LogManager.getLogger(DropIndexesReplImpl.class);


    @Override
    public Status<DropIndexesResult> apply(
            Request req,
            Command<? super DropIndexesArgument, ? super DropIndexesResult> command,
            DropIndexesArgument arg,
            SharedWriteTorodTransaction trans) {
        int indexesBefore = (int) trans.getIndexesInfo(req.getDatabase(), arg.getCollection()).count();
        
        List<String> indexesToDrop;
        
        if (!arg.isDropAllIndexes()) {
            if (!arg.isDropByKeys()) {
                if (Constants.ID_INDEX.equals(arg.getIndexToDrop())) {
                    LOGGER.warn("Trying to drop index {}. Ignoring the whole request",
                            arg.getIndexToDrop());
                }
                indexesToDrop = Arrays.asList(arg.getIndexToDrop());
            } else {
                if (arg.getKeys().stream().anyMatch(key -> !(KnownType.contains(key.getType())) ||
                        (key.getType() != KnownType.asc.getIndexType() &&
                        key.getType() != KnownType.desc.getIndexType()))) {
                    return getStatusForIndexNotFoundWithKeys(arg);
                }
                
                indexesToDrop = trans.getIndexesInfo(req.getDatabase(), arg.getCollection())
                    .filter(index -> indexFieldsMatchKeys(index, arg.getKeys()))
                    .map(index -> index.getName())
                    .collect(Collectors.toList());
                
                if (indexesToDrop.isEmpty()) {
                    return getStatusForIndexNotFoundWithKeys(arg);
                }
            }
        } else {
            indexesToDrop = trans.getIndexesInfo(req.getDatabase(), arg.getCollection())
                .filter(indexInfo -> !Constants.ID_INDEX.equals(indexInfo.getName()))
                .map(indexInfo -> indexInfo.getName())
                .collect(Collectors.toList());   
        }
        
        for (String indexToDrop : indexesToDrop) {
            boolean dropped = trans.dropIndex(
                    req.getDatabase(),
                    arg.getCollection(),
                    indexToDrop
            );
            if (!dropped) {
                LOGGER.warn("Trying to drop index {}, but it has not been "
                        + "found. Ignoring it", indexToDrop);
            }
        }

        return Status.ok(new DropIndexesResult(indexesBefore));
    }

    private Status<DropIndexesResult> getStatusForIndexNotFoundWithKeys(DropIndexesArgument arg) {
        return Status.from(ErrorCode.INDEX_NOT_FOUND, "index not found with keys [" + arg.getKeys()
            .stream()
            .map(key -> '"' + key.getKeys()
                    .stream()
                    .collect(Collectors.joining(".")) + "\" :" + key.getType().toBsonValue().toString())
            .collect(Collectors.joining(", ")) + "]");
    }
    
    private boolean indexFieldsMatchKeys(IndexInfo index, List<IndexOptions.Key> keys) {
        if (index.getFields().size() != keys.size()) {
            return false;
        }
        
        Iterator<IndexFieldInfo> fieldsIterator = index.getFields().iterator();
        Iterator<IndexOptions.Key> keysIterator = keys.iterator();
        while (fieldsIterator.hasNext()) {
            IndexFieldInfo field = fieldsIterator.next();
            IndexOptions.Key key = keysIterator.next();
            
            if ((field.isAscending() && key.getType() != KnownType.asc.getIndexType()) ||
                    (!field.isAscending() && key.getType() != KnownType.desc.getIndexType()) ||
                    (field.getAttributeReference().getKeys().size() != key.getKeys().size())) {
                return false;
            }
            
            Iterator<AttributeReference.Key<?>> fieldKeysIterator = field.getAttributeReference().getKeys().iterator();
            Iterator<String> keyKeysIterator = key.getKeys().iterator();
            
            while (fieldKeysIterator.hasNext()) {
                AttributeReference.Key<?> fieldKey = fieldKeysIterator.next();
                String keyKey = keyKeysIterator.next();
                
                if (!fieldKey.toString().equals(keyKey)) {
                    return false;
                }
            }
        }
        
        return true;
    }

}