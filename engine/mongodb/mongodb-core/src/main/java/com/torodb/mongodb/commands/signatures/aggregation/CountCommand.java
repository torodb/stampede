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

package com.torodb.mongodb.commands.signatures.aggregation;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonType;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.NumberField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.torodb.mongodb.commands.signatures.aggregation.CountCommand.CountArgument;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class CountCommand extends AbstractNotAliasableCommand<CountArgument, Long> {

  private static final NumberField N_FIELD = new NumberField("n");

  public static final CountCommand INSTANCE = new CountCommand();

  private CountCommand() {
    super("count");
  }

  @Override
  public Class<? extends CountArgument> getArgClass() {
    return CountArgument.class;
  }

  @Override
  public CountArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return CountArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(CountArgument request) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @Override
  public Class<? extends Long> getResultClass() {
    return Long.class;
  }

  @Override
  public BsonDocument marshallResult(Long reply) {
    return new BsonDocumentBuilder()
        .appendNumber(N_FIELD, reply)
        .build();
  }

  @Override
  public Long unmarshallResult(BsonDocument resultDoc) throws TypesMismatchException,
      NoSuchKeyException {
    return BsonReaderTool.getNumeric(resultDoc, N_FIELD).longValue();
  }

  public static class CountArgument {

    private final BsonDocument query; //TODO(gortiz) parse query
    private final String collection;
    @Nullable
    private final String hint;
    private final long limit;
    private final long skip;

    public CountArgument(
        @Nonnull String collection,
        @Nonnull BsonDocument query,
        @Nullable String hint,
        @Nonnegative long limit,
        @Nonnegative long skip) {
      this.collection = collection;
      this.query = query;
      this.hint = hint;
      this.limit = limit;
      this.skip = skip;
    }

    /**
     * A query that selects which documents to count in a collection.
     *
     * @return the filter or null if all documents shall be count
     */
    @Nullable
    public BsonDocument getQuery() {
      return query;
    }

    @Nonnegative
    public long getLimit() {
      return limit;
    }

    @Nonnegative
    public long getSkip() {
      return skip;
    }

    /**
     * The index to use. Specify either the index name as a string.
     *
     * @return
     */
    @Nullable
    public String getHint() {
      return hint;
    }

    public String getCollection() {
      return collection;
    }

    private static final StringField COUNT_FIELD = new StringField("count");
    private static final LongField SKIP_FIELD = new LongField("skip");
    private static final LongField LIMIT_FIELD = new LongField("limit");
    private static final DocField QUERY_FIELD = new DocField("query");
    private static final String HINT_FIELD_NAME = "hint";

    public static CountArgument unmarshall(BsonDocument doc) throws TypesMismatchException,
        BadValueException, NoSuchKeyException {
      long skip = BsonReaderTool.getLong(doc, SKIP_FIELD, 0);
      if (skip < 0) {
        throw new BadValueException("Skip value is negative in the count query");
      }

      long limit = BsonReaderTool.getLong(doc, LIMIT_FIELD, 0);
      if (limit < 0) {
        // For counts, limit and -limit mean the same thing.
        limit = -limit;
      }

      BsonDocument query;
      try {
        query = BsonReaderTool.getDocument(doc, QUERY_FIELD);
      } catch (NoSuchKeyException ex) {
        query = DefaultBsonValues.EMPTY_DOC;
      } catch (TypesMismatchException ex) {
        //Some drivers send non object values on query field when no query is specified
        //see mongo SERVER-15456
        query = DefaultBsonValues.EMPTY_DOC;
      }

      String hint = null;
      if (doc.containsKey(HINT_FIELD_NAME)) {
        BsonValue uncastedHint = doc.get(HINT_FIELD_NAME);
        if (uncastedHint.getType().equals(BsonType.STRING)) {
          hint = uncastedHint.asString().getValue();
        } else if (uncastedHint.getType().equals(BsonType.DOCUMENT)) {
          BsonDocument docHint = uncastedHint.asDocument();
          if (!docHint.isEmpty()) {
            hint = docHint.getFirstEntry().getKey();
          }
        }
      }

      String collection = BsonReaderTool.getString(doc, COUNT_FIELD);

      return new CountArgument(collection, query, hint, limit, skip);
    }
  }

}
