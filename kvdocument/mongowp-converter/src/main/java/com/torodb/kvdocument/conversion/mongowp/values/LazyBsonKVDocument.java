/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with mongowp-converter. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.kvdocument.conversion.mongowp.values;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import java.util.NoSuchElementException;
import javax.annotation.Nonnegative;

/**
 *
 */
public class LazyBsonKVDocument extends KVDocument {
    private static final EntryTranslateFunction ENTRY_TRANSLATE_FUNCTION = new EntryTranslateFunction();
    private static final long serialVersionUID = 5869442906673694930L;

    private final BsonDocument wrapped;

    public LazyBsonKVDocument(BsonDocument wrapped) {
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
    public KVValue<?> get(String key) {
        BsonValue<?> bsonValue = wrapped.get(key);
        return MongoWPConverter.translate(bsonValue);
    }

    private static class EntryTranslateFunction implements Function<BsonDocument.Entry<?>, KVDocument.DocEntry<?>> {

        @Override
        @SuppressWarnings("unchecked")
        public DocEntry<?> apply(@Nonnegative Entry<?> input) {
            return new MyDocEntry(input);
        }

    }

    private static class MyDocEntry<V> extends DocEntry {

        private final BsonDocument.Entry<?> entry;

        public MyDocEntry(Entry<?> entry) {
            this.entry = entry;
        }

        @Override
        public String getKey() {
            return entry.getKey();
        }

        @Override
        public KVValue getValue() {
            return MongoWPConverter.translate(entry.getValue());
        }
    }

}
