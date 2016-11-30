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

package com.torodb.mongodb.commands.signatures.diagnostic;

import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonType;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.annotations.NotMutable;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.ArrayField;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.Lists;
import com.torodb.mongodb.commands.signatures.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.torodb.mongodb.commands.tools.EmptyCommandArgumentMarshaller;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListDatabasesCommand extends AbstractNotAliasableCommand<Empty, ListDatabasesReply> {

  private static final org.apache.logging.log4j.Logger LOGGER =
      LogManager.getLogger(ListDatabasesCommand.class);
  public static final ListDatabasesCommand INSTANCE = new ListDatabasesCommand();
  private static final String COMMAND_NAME = "listDatabases";

  public ListDatabasesCommand() {
    super(COMMAND_NAME);
  }

  @Override
  public boolean isAdminOnly() {
    return true;
  }

  @Override
  public Class<? extends Empty> getArgClass() {
    return Empty.class;
  }

  @Override
  public Empty unmarshallArg(BsonDocument requestDoc) {
    return Empty.getInstance();
  }

  @Override
  public BsonDocument marshallArg(Empty request) {
    return EmptyCommandArgumentMarshaller.marshallEmptyArgument(this);
  }

  @Override
  public Class<? extends ListDatabasesReply> getResultClass() {
    return ListDatabasesReply.class;
  }

  @Override
  public BsonDocument marshallResult(ListDatabasesReply reply) {
    return reply.marshall();
  }

  @Override
  public ListDatabasesReply unmarshallResult(BsonDocument replyDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return ListDatabasesReply.unmarshall(replyDoc);
  }

  public static class ListDatabasesReply {

    private static final ArrayField DATABASES_FIELD = new ArrayField("databases");
    private static final LongField TOTAL_SIZE_FIELD = new LongField("totalSize");
    private static final LongField TOTAL_SIZE_MB_FIELD = new LongField("totalSizeMb");

    private final List<DatabaseEntry> databases;
    private final long totalSize;

    public ListDatabasesReply(@NotMutable List<DatabaseEntry> databases, long sizeOnDisk) {
      this.databases = Collections.unmodifiableList(databases);
      this.totalSize = sizeOnDisk;

      long temp = 0;
      for (DatabaseEntry database : databases) {
        temp += database.getSizeOnDisk();
      }
      if (temp != sizeOnDisk) {
        LOGGER.warn("Inconsistent data provided to " + getClass()
            + " constructor. Recived a total size of {} but {} was "
            + "calculated. Using provided value",
            sizeOnDisk,
            temp);
      }
    }

    public List<DatabaseEntry> getDatabases() {
      return databases;
    }

    /**
     *
     * @return the total size of all databases file on disk in bytes
     */
    public long getTotalSize() {
      return totalSize;
    }

    private static ListDatabasesReply unmarshall(
        BsonDocument replyDoc) throws TypesMismatchException, NoSuchKeyException {
      BsonArray arr = BsonReaderTool.getArray(replyDoc, DATABASES_FIELD);

      int i = 0;
      ArrayList<DatabaseEntry> databases = Lists.newArrayListWithCapacity(arr.size());
      for (BsonValue element : arr) {
        if (!element.isDocument()) {
          String field = DATABASES_FIELD.getFieldName() + "." + i;
          throw new TypesMismatchException(
              field,
              BsonType.DOCUMENT,
              element.getType(),
              "Element " + field + " is not a document as it was "
              + "expected but a " + element.getType());
        }
        databases.add(new DatabaseEntry(element.asDocument()));
        i++;
      }

      long sizeOnDisk = BsonReaderTool.getNumeric(replyDoc, TOTAL_SIZE_FIELD).longValue();
      return new ListDatabasesReply(databases, sizeOnDisk);
    }

    private BsonDocument marshall() {
      BsonArrayBuilder arr = new BsonArrayBuilder();
      for (DatabaseEntry database : databases) {
        arr.add(database.marshall());
      }

      return new BsonDocumentBuilder()
          .append(DATABASES_FIELD, arr.build())
          .append(TOTAL_SIZE_FIELD, totalSize)
          .append(TOTAL_SIZE_MB_FIELD, totalSize / (1000 * 1000))
          .build();
    }

    public static class DatabaseEntry {

      private static final StringField NAME_FIELD = new StringField("name");
      private static final LongField SIZE_ON_DISK_FIELD = new LongField("sizeOnDisk");
      private static final BooleanField EMPTY_FIELD = new BooleanField("empty");

      private final String name;
      private final long sizeOnDisk;
      private final boolean empty;

      public DatabaseEntry(String name, long sizeOnDisk, boolean empty) {
        this.name = name;
        this.sizeOnDisk = sizeOnDisk;
        this.empty = empty;
      }

      private DatabaseEntry(BsonDocument doc) throws TypesMismatchException, NoSuchKeyException {
        name = BsonReaderTool.getString(doc, NAME_FIELD);
        sizeOnDisk = BsonReaderTool.getNumeric(doc, SIZE_ON_DISK_FIELD).longValue();
        empty = BsonReaderTool.getBoolean(doc, EMPTY_FIELD);
      }

      /**
       *
       * @return the name of the database
       */
      public String getName() {
        return name;
      }

      /**
       *
       * @return the total size of the database file on disk in bytes
       */
      public long getSizeOnDisk() {
        return sizeOnDisk;
      }

      /**
       *
       * @return whether the databse has data
       */
      public boolean isEmpty() {
        return empty;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(NAME_FIELD, name)
            .append(SIZE_ON_DISK_FIELD, sizeOnDisk)
            .append(EMPTY_FIELD, empty)
            .build();
      }
    }
  }
}
