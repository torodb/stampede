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

package com.torodb.mongodb.repl.commands;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.FailedToParseException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.impl.AbstractCommand;
import com.eightkdata.mongowp.server.api.tools.Empty;

public class LogAndIgnoreCommand extends AbstractCommand<String, Empty> {

  public static final LogAndIgnoreCommand INSTANCE = new LogAndIgnoreCommand();

  private LogAndIgnoreCommand() {
    super("log-and-ignore");
  }

  @Override
  public Class<? extends String> getArgClass() {
    return String.class;
  }

  @Override
  public String unmarshallArg(BsonDocument requestDoc, String aliasedAs) throws MongoException {
    return requestDoc.getFirstEntry().getKey();
  }

  @Override
  public BsonDocument marshallArg(String request, String aliasedAs) throws MarshalException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Class<? extends Empty> getResultClass() {
    return Empty.class;
  }

  @Override
  public Empty unmarshallResult(BsonDocument resultDoc) throws
      BadValueException, TypesMismatchException, NoSuchKeyException,
      FailedToParseException, MongoException {
    return Empty.getInstance();
  }

  @Override
  public BsonDocument marshallResult(Empty result) throws MarshalException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
