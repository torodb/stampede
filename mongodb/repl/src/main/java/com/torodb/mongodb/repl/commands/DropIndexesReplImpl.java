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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand.DropIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand.DropIndexesResult;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.language.Constants;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
        int indexesBefore = (int) trans
                .getIndexesInfo(req.getDatabase(), arg.getCollection())
                .count();

        List<String> indexesToDrop;

        if (Constants.ID_INDEX.equals(arg.getIndexToDrop())) {
            LOGGER.warn("Trying to drop index {}. Ignoring the whole request",
                    arg.getIndexToDrop());
            return Status.from(ErrorCode.INVALID_OPTIONS, "cannot drop _id index");
        }

        if (!arg.isDropAllIndexes()) {
            indexesToDrop = Arrays.asList(arg.getIndexToDrop());
        } else {
            indexesToDrop = trans.getIndexesInfo(req.getDatabase(), arg.getCollection())
                            .filter(indexInfo
                                    -> !Constants.ID_INDEX.equals(indexInfo.getName()))
                            .map(indexInfo -> indexInfo.getName())
                            .collect(Collectors.toList());
        }

        for (String indexToDrop : indexesToDrop) {
            if (!trans.dropIndex(req.getDatabase(), arg.getCollection(), indexToDrop)) {
                LOGGER.warn("Trying to drop index {}, but it has not been "
                        + "found. Ignoring it", indexToDrop);
            }
        }

        return Status.ok(new DropIndexesResult(indexesBefore));

    }

}
