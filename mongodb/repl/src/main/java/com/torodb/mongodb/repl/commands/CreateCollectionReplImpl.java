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
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.mongodb.language.Constants;
import com.torodb.torod.IndexFieldInfo;
import java.util.Arrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.torodb.torod.SharedWriteTorodTransaction;

/**
 *
 */
public class CreateCollectionReplImpl extends ReplCommandImpl<CreateCollectionArgument, Empty> {
    private static final Logger LOGGER
            = LogManager.getLogger(CreateCollectionReplImpl.class);

    @Override
    public Status<Empty> apply(
            Request req,
            Command<? super CreateCollectionArgument, ? super Empty> command,
            CreateCollectionArgument arg,
            SharedWriteTorodTransaction trans) {

        try {
            if (!trans.existsCollection(req.getDatabase(), arg.getCollection())) {
                trans.createIndex(
                        req.getDatabase(),
                        arg.getCollection(),
                        Constants.ID_INDEX,
                        ImmutableList.of(
                                new IndexFieldInfo(
                                        new AttributeReference(
                                                Arrays.asList(new Key[] {
                                                    new ObjectKey(Constants.ID) })
                                        ),
                                        FieldIndexOrdering.ASC.isAscending()
                                )
                        ), true
                );
            }

            trans.createCollection(req.getDatabase(), arg.getCollection());
        } catch (UserException ex) {
            reportErrorIgnored(LOGGER, command, ex);
        }
        return Status.ok();
    }

}
