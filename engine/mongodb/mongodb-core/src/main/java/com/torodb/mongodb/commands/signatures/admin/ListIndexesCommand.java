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

package com.torodb.mongodb.commands.signatures.admin;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.torodb.mongodb.commands.pojos.CursorResult;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand.ListIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand.ListIndexesResult;

import javax.annotation.Nonnull;

/**
 *
 */
public class ListIndexesCommand
    extends AbstractNotAliasableCommand<ListIndexesArgument, ListIndexesResult> {

  public static final ListIndexesCommand INSTANCE = new ListIndexesCommand();
  private static final String COMMAND_NAME = "listIndexes";

  private ListIndexesCommand() {
    super(COMMAND_NAME);
  }

  @Override
  public Class<? extends ListIndexesArgument> getArgClass() {
    return ListIndexesArgument.class;
  }

  @Override
  public ListIndexesArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return ListIndexesArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(ListIndexesArgument request) {
    return request.marshall(request);
  }

  @Override
  public Class<? extends ListIndexesResult> getResultClass() {
    return ListIndexesResult.class;
  }

  @Override
  public BsonDocument marshallResult(ListIndexesResult reply) throws MarshalException {
    try {
      return reply.marshall();
    } catch (MongoException ex) {
      throw new MarshalException(ex);
    }
  }

  @Override
  public ListIndexesResult unmarshallResult(BsonDocument replyDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return ListIndexesResult.unmarshall(replyDoc);
  }

  public static class ListIndexesArgument {

    private static final StringField COL_NAME_FIELD = new StringField("listIndexes");
    private final String collection;

    public ListIndexesArgument(String collection) {
      this.collection = collection;
    }

    @Nonnull
    public String getCollection() {
      return collection;
    }

    private static ListIndexesArgument unmarshall(BsonDocument requestDoc)
        throws TypesMismatchException, NoSuchKeyException, BadValueException {
      try {
        String colName = BsonReaderTool.getString(requestDoc, COL_NAME_FIELD);
        if (colName.isEmpty()) {
          throw new BadValueException("Argument to listIndexes must be "
              + "a collection name, not the empty string");
        }
        return new ListIndexesArgument(colName);
      } catch (TypesMismatchException ex) {
        throw ex.newWithMessage("Argument to listIndexes must be of "
            + "type String, not " + ex.getFoundType());
      }
    }

    private BsonDocument marshall(ListIndexesArgument request) {
      return new BsonDocumentBuilder()
          .append(COL_NAME_FIELD, collection)
          .build();
    }

  }

  public static class ListIndexesResult {

    private static final DocField CURSOR_FIELD = new DocField("cursor");
    private final CursorResult<IndexOptions> cursor;

    public ListIndexesResult(CursorResult<IndexOptions> cursor) {
      this.cursor = cursor;
    }

    public CursorResult<IndexOptions> getCursor() {
      return cursor;
    }

    private static ListIndexesResult unmarshall(BsonDocument reply)
        throws TypesMismatchException, NoSuchKeyException, BadValueException {
      BsonDocument cursorDoc = BsonReaderTool.getDocument(reply, CURSOR_FIELD);

      return new ListIndexesResult(
          CursorResult.unmarshall(cursorDoc, IndexOptions.UNMARSHALLER_FUN)
      );
    }

    private BsonDocument marshall() throws MongoException {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();

      return builder
          .append(CURSOR_FIELD, cursor.marshall(IndexOptions.MARSHALLER_FUN))
          .build();
    }

  }
}
