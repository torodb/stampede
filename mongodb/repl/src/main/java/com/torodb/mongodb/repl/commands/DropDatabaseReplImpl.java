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

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.core.exceptions.user.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.torodb.torod.ExclusiveWriteTorodTransaction;
import com.torodb.torod.SharedWriteTorodTransaction;

/**
 *
 */
public class DropDatabaseReplImpl extends ReplCommandImpl<Empty, Empty> {
    private static final Logger LOGGER
            = LogManager.getLogger(DropDatabaseReplImpl.class);

    @Override
    public Status<Empty> apply(Request req,
            Command<? super Empty, ? super Empty> command, Empty arg,
            ExclusiveWriteTorodTransaction trans) {
        try {
            LOGGER.info("Dropping database {}", req.getDatabase());

            if (trans.existsDatabase(req.getDatabase())) {
                trans.dropDatabase(req.getDatabase());
            } else {
                LOGGER.info("Trying to drop database " + req.getDatabase() + " but it has not been found. "
                        + "This is normal since the database could have a collection being filtered "
                        + "or we are reapplying oplog during a recovery. Ignoring operation");
            }
        } catch (UserException ex) {
            reportErrorIgnored(LOGGER, command, ex);
        }

        return Status.ok();
    }

}
