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
import com.eightkdata.mongowp.exceptions.InvalidNamespaceException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.messages.utils.NamespaceUtils;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.torodb.mongodb.commands.signatures.admin.RenameCollectionCommand.RenameCollectionArgument;

/**
 *
 */
public class RenameCollectionCommand
    extends AbstractNotAliasableCommand<RenameCollectionArgument, Empty> {

  private static final String COMMAND_NAME = "renameCollection";
  public static final RenameCollectionCommand INSTANCE = new RenameCollectionCommand();

  public RenameCollectionCommand() {
    super(COMMAND_NAME);
  }

  @Override
  public Class<? extends RenameCollectionArgument> getArgClass() {
    return RenameCollectionArgument.class;
  }

  @Override
  public RenameCollectionArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return RenameCollectionArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(RenameCollectionArgument request) {
    return request.marshall();
  }

  @Override
  public Class<? extends Empty> getResultClass() {
    return Empty.class;
  }

  @Override
  public BsonDocument marshallResult(Empty reply) {
    return null;
  }

  @Override
  public Empty unmarshallResult(BsonDocument replyDoc) throws TypesMismatchException,
      NoSuchKeyException {
    return Empty.getInstance();
  }

  public static class RenameCollectionArgument {

    private static final StringField FROM_NAMESPACE_FIELD = new StringField(COMMAND_NAME);
    private static final StringField TO_NAMESPACE_FIELD = new StringField("to");
    private static final BooleanField DROP_TARGET_FIELD = new BooleanField("dropTarget");

    private final String fromDatabase;
    private final String fromCollection;
    private final String toDatabase;
    private final String toCollection;
    private final boolean dropTarget;

    public RenameCollectionArgument(String fromDatabase, String fromCollection, String toDatabase,
        String toCollection, boolean dropTarget) {
      this.fromDatabase = fromDatabase;
      this.fromCollection = fromCollection;
      this.toDatabase = toDatabase;
      this.toCollection = toCollection;
      this.dropTarget = dropTarget;
    }

    public String getFromDatabase() {
      return fromDatabase;
    }

    public String getFromCollection() {
      return fromCollection;
    }

    public String getToDatabase() {
      return toDatabase;
    }

    public String getToCollection() {
      return toCollection;
    }

    public boolean isDropTarget() {
      return dropTarget;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(FROM_NAMESPACE_FIELD, NamespaceUtils.get(fromDatabase, fromCollection))
          .append(TO_NAMESPACE_FIELD, NamespaceUtils.get(toDatabase, toCollection))
          .append(DROP_TARGET_FIELD, dropTarget)
          .build();
    }

    private static RenameCollectionArgument unmarshall(BsonDocument requestDoc)
        throws TypesMismatchException, NoSuchKeyException, BadValueException {
      String fromNamespace = BsonReaderTool.getString(requestDoc, FROM_NAMESPACE_FIELD);
      String toNamespace = BsonReaderTool.getString(requestDoc, TO_NAMESPACE_FIELD);
      boolean dropTarget = BsonReaderTool
          .getBooleanOrUndefined(requestDoc, DROP_TARGET_FIELD, false);

      String fromCollection;
      String fromDatabase;
      try {
        fromDatabase = NamespaceUtils.getDatabase(fromNamespace);
        fromCollection = NamespaceUtils.getCollection(fromNamespace);
      } catch (InvalidNamespaceException invalidNamespaceException) {
        throw new BadValueException(invalidNamespaceException.getMessage(),
            invalidNamespaceException);
      }

      String toCollection;
      String toDatabase;
      try {
        toDatabase = NamespaceUtils.getDatabase(toNamespace);
        toCollection = NamespaceUtils.getCollection(toNamespace);
      } catch (InvalidNamespaceException invalidNamespaceException) {
        throw new BadValueException(invalidNamespaceException.getMessage(),
            invalidNamespaceException);
      }

      return new RenameCollectionArgument(
          fromDatabase, fromCollection, toDatabase, toCollection, dropTarget);
    }
  }

}
