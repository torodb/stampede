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

import java.util.List;
import java.util.stream.Collectors;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CursorResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions.IndexType;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions.IndexVersion;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.core.MongodTransaction;
import com.torodb.torod.IndexInfo.IndexFieldInfo;

public class ListIndexesImplementation implements ReadTorodbCommandImpl<ListIndexesArgument, ListIndexesResult> {

    @Override
    public Status<ListIndexesResult> apply(Request req, Command<? super ListIndexesArgument, ? super ListIndexesResult> command,
        ListIndexesArgument arg, MongodTransaction context) {
        return Status.ok(new ListIndexesResult(
                CursorResult.createSingleBatchCursor(req.getDatabase(), arg.getCollection(), 
                        context.getTorodTransaction().getIndexesInfo(req.getDatabase(), arg.getCollection())
                        .map(indexInfo -> 
                                new IndexOptions(
                                        IndexVersion.V1,
                                        indexInfo.getName(),
                                        req.getDatabase(),
                                        arg.getCollection(),
                                        false,
                                        indexInfo.isUnique(),
                                        false,
                                        0,
                                        indexInfo.getFields().stream().collect(Collectors.toMap(
                                                indexFieldInfo -> extractKeys(indexFieldInfo), 
                                                indexFieldInfo -> extractType(indexFieldInfo))),
                                        null,
                                        null)
                        )
                )
            ));
    }

    private List<String> extractKeys(IndexFieldInfo indexFieldInfo) {
        return indexFieldInfo.getAttributeReference().getKeys().stream()
            .map(k -> k.getKeyValue().toString()).collect(Collectors.toList());
    }

    private IndexType extractType(IndexFieldInfo indexFieldInfo) {
        return indexFieldInfo.isAscending() ? IndexType.asc : IndexType.desc;
    }
    
}
