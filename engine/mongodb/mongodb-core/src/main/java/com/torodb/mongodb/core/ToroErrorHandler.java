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

package com.torodb.mongodb.core;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.MongoConstants;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.fields.DoubleField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.messages.request.EmptyBsonContext;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.messages.utils.IterableDocumentProvider;
import com.eightkdata.mongowp.server.api.Connection;
import com.eightkdata.mongowp.server.api.ErrorHandler;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;

/**
 *
 */
public class ToroErrorHandler implements ErrorHandler {

  private static final StringField ERRMSG_FIELD = new StringField("errmsg");
  private static final IntField CODE_FIELD = new IntField("code");
  private static final DoubleField OK_FIELD = new DoubleField("ok");

  private static final Logger LOGGER = LogManager.getLogger(ToroErrorHandler.class);

  @Override
  @Nullable
  public ReplyMessage handleUnexpectedError(
      Connection connection,
      int requestId,
      boolean canReply,
      Throwable error) {
    if (canReply) {
      LOGGER.warn(
          "An unexpected error was catched",
          error
      );
      return new ReplyMessage(
          EmptyBsonContext.getInstance(),
          requestId,
          false,
          false,
          false,
          false,
          0,
          0,
          IterableDocumentProvider.of(
              new BsonDocumentBuilder(3)
                  .append(ERRMSG_FIELD, "An unexpected error was catched")
                  .append(CODE_FIELD, ErrorCode.UNKNOWN_ERROR.getErrorCode())
                  .append(OK_FIELD, MongoConstants.KO)
                  .build()
          )
      );
    } else {
      LOGGER.warn(
          "An error was catched but it cannot be returned to the user",
          error
      );
      return null;
    }
  }

  @Override
  @Nullable
  public ReplyMessage handleMongodbException(
      Connection connection,
      int requestId,
      boolean canReply,
      MongoException exception) {
    if (canReply) {
      return new ReplyMessage(
          EmptyBsonContext.getInstance(),
          requestId,
          false,
          false,
          false,
          false,
          0,
          0,
          IterableDocumentProvider.of(
              new BsonDocumentBuilder()
                  .append(ERRMSG_FIELD, exception.getMessage())
                  .append(CODE_FIELD, exception.getErrorCode().getErrorCode())
                  .appendUnsafe(OK_FIELD.getFieldName(), MongoConstants.BSON_KO)
                  .build()
          )
      );
    } else {
      LOGGER.warn(
          "An error was catched but it cannot be returned to the user",
          exception
      );
      return null;
    }
  }

}
