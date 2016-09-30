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
import java.util.stream.Collectors;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand.DropIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand.DropIndexesResult;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.Constants;

public class DropIndexesImplementation implements WriteTorodbCommandImpl<DropIndexesArgument, DropIndexesResult> {

    @Override
    public Status<DropIndexesResult> apply(Request req, Command<? super DropIndexesArgument, ? super DropIndexesResult> command,
            DropIndexesArgument arg, WriteMongodTransaction context) {
        int indexesBefore = (int) context.getTorodTransaction().getIndexesInfo(req.getDatabase(), arg.getCollection()).count();
        
        List<String> indexesToDrop;
        
        if (!arg.isDropAllIndexes()) {
            if (Constants.ID_INDEX.equals(arg.getIndexToDrop())) {
                return Status.from(ErrorCode.INVALID_OPTIONS, "cannot drop _id index");
            }
            indexesToDrop = Arrays.asList(arg.getIndexToDrop());
        } else {
            indexesToDrop = context.getTorodTransaction().getIndexesInfo(req.getDatabase(), arg.getCollection())
                .filter(indexInfo -> !Constants.ID_INDEX.equals(indexInfo.getName()))
                .map(indexInfo -> indexInfo.getName())
                .collect(Collectors.toList());   
        }
        
        for (String indexToDrop : indexesToDrop) {
            if (!context.getTorodTransaction().dropIndex(req.getDatabase(), arg.getCollection(), indexToDrop)) {
                return Status.from(ErrorCode.INDEX_NOT_FOUND, "index not found with name [" + indexToDrop + "]");
            }
        }

        return Status.ok(new DropIndexesResult(indexesBefore));
    }

}
