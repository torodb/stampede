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
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.DoubleField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.torodb.mongodb.commands.pojos.CollectionOptions;
import com.torodb.mongodb.commands.pojos.CursorResult;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand.ListCollectionsArgument;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand.ListCollectionsResult;

import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ListCollectionsCommand
    extends AbstractNotAliasableCommand<ListCollectionsArgument, ListCollectionsResult> {

  public static final ListCollectionsCommand INSTANCE = new ListCollectionsCommand();
  private static final String COMMAND_NAME = "listCollections";

  private ListCollectionsCommand() {
    super(COMMAND_NAME);
  }

  @Override
  public boolean shouldAffectCommandCounter() {
    return false;
  }

  @Override
  public Class<? extends ListCollectionsArgument> getArgClass() {
    return ListCollectionsArgument.class;
  }

  @Override
  public ListCollectionsArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return ListCollectionsArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(ListCollectionsArgument request) {
    return request.marshall();
  }

  @Override
  public Class<? extends ListCollectionsResult> getResultClass() {
    return ListCollectionsResult.class;
  }

  @Override
  public BsonDocument marshallResult(ListCollectionsResult reply) throws MarshalException {
    try {
      return reply.marshall();
    } catch (MongoException ex) {
      throw new MarshalException(ex);
    }
  }

  @Override
  public ListCollectionsResult unmarshallResult(BsonDocument reply)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return ListCollectionsResult.unmarshall(reply);
  }

  @Immutable
  public static class ListCollectionsArgument {

    private static final DoubleField LIST_COLLECTIONS_FIELD = new DoubleField("listCollections");
    private static final DocField FILTER_FIELD = new DocField("filter");

    private final BsonDocument filter;

    public ListCollectionsArgument(@Nullable BsonDocument filter) {
      this.filter = filter;
    }

    @Nullable
    public BsonDocument getFilter() {
      return filter;
    }

    private static ListCollectionsArgument unmarshall(
        BsonDocument requestDoc) throws TypesMismatchException {
      BsonDocument filter;
      try {
        filter = BsonReaderTool.getDocument(requestDoc, FILTER_FIELD, null);
      } catch (TypesMismatchException ex) {
        throw ex.newWithMessage("\"filter\" must be of type Object, not " + ex.getFoundType());
      }
      return new ListCollectionsArgument(filter);
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();
      builder.append(LIST_COLLECTIONS_FIELD, 1);

      if (filter != null) {
        builder.append(FILTER_FIELD, filter);
      }

      return builder.build();
    }

  }

  @Immutable
  public static class ListCollectionsResult {

    private static final DocField CURSOR_FIELD = new DocField("cursor");
    private static final StringField NAME_FIELD = new StringField("name");
    private static final DocField OPTIONS_FIELD = new DocField("options");

    private final CursorResult<Entry> cursor;

    public ListCollectionsResult(CursorResult<Entry> cursor) {
      this.cursor = cursor;
    }

    private static ListCollectionsResult unmarshall(BsonDocument resultDoc)
        throws TypesMismatchException, NoSuchKeyException, BadValueException {

      BsonDocument cursorDoc = BsonReaderTool.getDocument(resultDoc, CURSOR_FIELD);

      return new ListCollectionsResult(
          CursorResult.unmarshall(cursorDoc, Entry.TO_ENTRY)
      );
    }

    private BsonDocument marshall() throws MongoException {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();

      return builder
          .append(CURSOR_FIELD, cursor.marshall(Entry.FROM_ENTRY))
          .build();
    }

    public CursorResult<Entry> getCursor() {
      return cursor;
    }

    public static class Entry {

      private final String name;
      private final CollectionOptions options;
      public static final Function<BsonValue<?>, Entry> TO_ENTRY = new ToEntryConverter();
      public static final Function<Entry, BsonDocument> FROM_ENTRY = new FromEntryConverter();

      public Entry(String name, CollectionOptions options) {
        this.name = name;
        this.options = options;
      }

      public String getCollectionName() {
        return name;
      }

      public CollectionOptions getCollectionOptions() {
        return options;
      }
    }

    private static class ToEntryConverter implements Function<BsonValue<?>, Entry> {

      @Override
      public Entry apply(@Nonnull BsonValue<?> from) {
        if (!from.isDocument()) {
          throw new IllegalArgumentException("Expected a document, "
              + "but a " + from.getType() + " was found");
        }

        try {
          BsonDocument doc = from.asDocument();
          return new Entry(
              BsonReaderTool.getString(doc, NAME_FIELD),
              CollectionOptions.unmarshal(
                  BsonReaderTool.getDocument(doc, OPTIONS_FIELD)
              )
          );
        } catch (TypesMismatchException | NoSuchKeyException | BadValueException ex) {
          throw new IllegalArgumentException(ex);
        }
      }
    }

    private static class FromEntryConverter implements Function<Entry, BsonDocument> {

      @Override
      public BsonDocument apply(@Nonnull Entry from) {

        return new BsonDocumentBuilder()
            .append(NAME_FIELD, from.getCollectionName())
            .append(OPTIONS_FIELD,
                from.getCollectionOptions().marshall()
            )
            .build();
      }

    }
  }
}
