/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.repl.commands.impl;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.torodb.mongodb.utils.DefaultIdUtils;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.SharedWriteTorodTransaction;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

import javax.inject.Inject;

public class CreateCollectionReplImpl extends ReplCommandImpl<CreateCollectionArgument, Empty> {

  private final Logger logger;
  private final CommandFilterUtil filterUtil;

  @Inject
  public CreateCollectionReplImpl(CommandFilterUtil filterUtil, LoggerFactory loggerFactory) {
    this.filterUtil = filterUtil;
    this.logger = loggerFactory.apply(this.getClass());
  }

  @Override
  public Status<Empty> apply(
      Request req,
      Command<? super CreateCollectionArgument, ? super Empty> command,
      CreateCollectionArgument arg,
      SharedWriteTorodTransaction trans) {

    if (!filterUtil.testNamespaceFilter(req.getDatabase(), arg.getCollection(), command)) {
      return Status.ok();
    }

    try {
      logger.info("Creating collection {}.{}", req.getDatabase(), arg.getCollection());

      if (arg.getOptions().isCapped()) {
        logger.info("Ignoring capped flag for collection {}.{}", req.getDatabase(), arg
            .getCollection());
      }

      if (!trans.existsCollection(req.getDatabase(), arg.getCollection())) {
        trans.createIndex(
            req.getDatabase(),
            arg.getCollection(),
            DefaultIdUtils.ID_INDEX,
            ImmutableList.of(
                new IndexFieldInfo(
                    new AttributeReference(
                        Arrays.asList(new Key[]{
                          new ObjectKey(DefaultIdUtils.ID_KEY)})),
                    FieldIndexOrdering.ASC.isAscending()
                )
            ), true
        );
      }

      trans.createCollection(req.getDatabase(), arg.getCollection());
    } catch (UserException ex) {
      reportErrorIgnored(logger, command, ex);
    }
    return Status.ok();
  }

}
