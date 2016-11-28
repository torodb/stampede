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

package com.torodb.mongodb.commands.pojos;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.ArrayField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class CursorResult<E> {

  private static final LongField ID_FIELD = new LongField("id");
  private static final StringField NAMESPACE_FIELD = new StringField("ns");
  private static final ArrayField FIRST_BATCH_FIELD = new ArrayField("firstBatch");

  public BsonDocument marshall(Function<E, ? extends BsonValue<?>> transformation) {
    BsonArrayBuilder array = new BsonArrayBuilder();
    Iterator<E> firstBatch = getFirstBatch();
    while (firstBatch.hasNext()) {
      array.add(transformation.apply(firstBatch.next()));
    }

    return new BsonDocumentBuilder()
        .append(ID_FIELD, getCursorId())
        .append(NAMESPACE_FIELD, getDatabase() + '.' + getCollection())
        .append(FIRST_BATCH_FIELD, array.build())
        .build();
  }

  public static <E> CursorResult<E> createSingleBatchCursor(String db, String col, Iterator<E> it) {
    return new DefaultCursorResult<>(db, col, 0, it);
  }

  public static <E> CursorResult<E> createSingleBatchCursor(String db, String col,
      Stream<E> stream) {
    return new DefaultCursorResult<>(db, col, 0, stream.iterator());
  }

  public static <E> CursorResult<E> unmarshall(BsonDocument doc,
      Function<BsonValue<?>, E> transformation)
      throws BadValueException, TypesMismatchException, NoSuchKeyException {

    String ns = BsonReaderTool.getString(doc, NAMESPACE_FIELD);
    int dotIndex = ns.indexOf('.');

    String database;
    String collection;

    if (dotIndex == -1) {
      throw new BadValueException("The given namespace (" + ns + ") does "
          + "not contain a collection ");
    } else {
      database = ns.substring(0, dotIndex);
      collection = ns.substring(dotIndex + 1);
    }

    ArrayList<E> list = new ArrayList<>();
    for (BsonValue<?> element : BsonReaderTool.getArray(doc, FIRST_BATCH_FIELD)) {
      list.add(transformation.apply(element));
    }

    return new DefaultCursorResult<>(
        database,
        collection,
        BsonReaderTool.getLong(doc, ID_FIELD),
        list.iterator()
    );
  }

  public abstract String getDatabase();

  public abstract String getCollection();

  public abstract long getCursorId();

  public abstract Iterator<E> getFirstBatch();

  protected static class DefaultCursorResult<E> extends CursorResult<E> {

    private final String database;
    private final String collection;
    private final long cursorId;
    private final Iterator<E> firstBatch;

    public DefaultCursorResult(String database, String collection, long cursorId,
        Iterator<E> firstBatch) {
      this.database = database;
      this.collection = collection;
      this.cursorId = cursorId;
      this.firstBatch = firstBatch;
    }

    @Override
    public String getDatabase() {
      return database;
    }

    @Override
    public String getCollection() {
      return collection;
    }

    @Override
    public long getCursorId() {
      return cursorId;
    }

    @Override
    public Iterator<E> getFirstBatch() {
      return firstBatch;
    }
  }

}
