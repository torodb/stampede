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

package com.torodb.kvdocument.conversion.mongowp.values;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;

import java.util.NoSuchElementException;

import javax.annotation.Nonnegative;

/**
 *
 */
public class LazyBsonKvDocument extends KvDocument {

  private static final EntryTranslateFunction ENTRY_TRANSLATE_FUNCTION =
      new EntryTranslateFunction();
  private static final long serialVersionUID = 5869442906673694930L;

  private final BsonDocument wrapped;

  public LazyBsonKvDocument(BsonDocument wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public UnmodifiableIterator<DocEntry<?>> iterator() {
    return Iterators.unmodifiableIterator(
        Iterators.transform(wrapped.iterator(), ENTRY_TRANSLATE_FUNCTION)
    );
  }

  @Override
  public DocEntry<?> getFirstEntry() throws NoSuchElementException {
    return ENTRY_TRANSLATE_FUNCTION.apply(wrapped.getFirstEntry());
  }

  @Override
  public KvValue<?> get(String key) {
    BsonValue<?> bsonValue = wrapped.get(key);
    return MongoWpConverter.translate(bsonValue);
  }

  private static class EntryTranslateFunction implements
      Function<BsonDocument.Entry<?>, KvDocument.DocEntry<?>> {

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public DocEntry<?> apply(@Nonnegative Entry<?> input) {
      return new MyDocEntry(input);
    }

  }

  private static class MyDocEntry<V> extends DocEntry<V> {

    private final BsonDocument.Entry<?> entry;

    public MyDocEntry(Entry<?> entry) {
      this.entry = entry;
    }

    @Override
    public String getKey() {
      return entry.getKey();
    }

    @SuppressWarnings("unchecked")
    @Override
    public KvValue<V> getValue() {
      return (KvValue<V>) MongoWpConverter.translate(entry.getValue());
    }
  }

}
