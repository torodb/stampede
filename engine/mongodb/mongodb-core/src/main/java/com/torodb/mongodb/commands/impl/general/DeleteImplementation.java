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

package com.torodb.mongodb.commands.impl.general;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Builder;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.commands.signatures.general.DeleteCommand.DeleteArgument;
import com.torodb.mongodb.commands.signatures.general.DeleteCommand.DeleteStatement;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.torod.SharedWriteTorodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class DeleteImplementation implements WriteTorodbCommandImpl<DeleteArgument, Long> {

  private static final Logger LOGGER = LogManager.getLogger(DeleteImplementation.class);

  private MongodMetrics mongodMetrics;

  @Inject
  public DeleteImplementation(MongodMetrics mongodMetrics) {
    this.mongodMetrics = mongodMetrics;
  }

  @Override
  public Status<Long> apply(Request req, Command<? super DeleteArgument, ? super Long> command,
      DeleteArgument arg,
      WriteMongodTransaction context) {
    Long deleted = 0L;

    for (DeleteStatement deleteStatement : arg.getStatements()) {
      BsonDocument query = deleteStatement.getQuery();

      switch (query.size()) {
        case 0: {
          deleted += context.getTorodTransaction()
              .deleteAll(req.getDatabase(), arg.getCollection());
          break;
        }
        case 1: {
          try {
            logDeleteCommand(arg);
            deleted += deleteByAttribute(context.getTorodTransaction(), req.getDatabase(), arg
                .getCollection(), query);
          } catch (CommandFailed ex) {
            return Status.from(ex);
          }
          break;
        }
        default: {
          return Status.from(ErrorCode.COMMAND_FAILED,
              "The given query is not supported right now");
        }
      }
    }
    mongodMetrics.getDeletes().mark(deleted);
    return Status.ok(deleted);

  }

  private long deleteByAttribute(SharedWriteTorodTransaction transaction, String db, String col,
      BsonDocument query) throws CommandFailed {
    Builder refBuilder = new AttributeReference.Builder();
    KvValue<?> kvValue = AttrRefHelper.calculateValueAndAttRef(query, refBuilder);
    return transaction.deleteByAttRef(db, col, refBuilder.build(), kvValue);
  }

  private void logDeleteCommand(DeleteArgument arg) {
    if (LOGGER.isTraceEnabled()) {
      String collection = arg.getCollection();
      String filter = StreamSupport.stream(arg.getStatements().spliterator(), false)
          .map(statement -> statement.getQuery().toString())
          .collect(Collectors.joining(","));

      LOGGER.trace("Delete from {} filter {}", collection, filter);
    }
  }

}
