/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */


package com.torodb.torod.core.utils;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.heap.InstantKVInstant;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.values.*;
import com.torodb.torod.core.subdocument.values.heap.*;
import java.util.Iterator;
import javax.annotation.Nonnull;

/**
 *
 */
public class KVValueToScalarValue {

    public static final Function<KVValue<?>, ScalarValue<?>> AS_FUNCTION = new Function<KVValue<?>, ScalarValue<?>>() {
        @Override
        public ScalarValue<?> apply(@Nonnull KVValue<?> input) {
            return fromDocValue(input);
        }
    };
    public static final KVValueVisitor<ScalarValue<?>, Void> AS_VISITOR = new FromKVValueVisitor();

    private KVValueToScalarValue() {
    }
    
    public static ScalarValue<?> fromDocValue(KVValue<?> docValue) {
        return docValue.accept(AS_VISITOR, null);
    }

    public static <N extends Number> ScalarNumeric<N> fromNumber(KVNumeric<N> kvValue) {
        return (ScalarNumeric<N>) fromDocValue(kvValue);
    }

    private static class FromKVValueVisitor implements KVValueVisitor<ScalarValue<?>, Void> {

        @Override
        public ScalarValue<?> visit(KVBoolean value, Void arg) {
            return ScalarBoolean.from(value.getPrimitiveValue());
        }

        @Override
        public ScalarValue<?> visit(KVNull value, Void arg) {
            return ScalarNull.getInstance();
        }

        @Override
        public ScalarValue<?> visit(KVArray value, Void arg) {
            return new KVScalarArray(value);
        }

        @Override
        public ScalarValue<?> visit(KVInteger value, Void arg) {
            return ScalarInteger.of(value.intValue());
        }

        @Override
        public ScalarValue<?> visit(KVLong value, Void arg) {
            return ScalarLong.of(value.longValue());
        }

        @Override
        public ScalarValue<?> visit(KVDouble value, Void arg) {
            return ScalarDouble.of(value.doubleValue());
        }

        @Override
        public ScalarValue<?> visit(KVString value, Void arg) {
            return new KVScalarString(value);
        }

        @Override
        public ScalarValue<?> visit(KVDocument value, Void arg) {
            throw new ToroImplementationException("DocValue "+ value + " is not translatable to scalar values");
        }

        @Override
        public ScalarValue<?> visit(KVMongoObjectId value, Void arg) {
            return new ByteArrayScalarMongoObjectId(value.getArrayValue());
        }

        @Override
        public ScalarValue<?> visit(KVInstant value, Void arg) {
            if (value instanceof InstantKVInstant) {
                return new InstantScalarInstant(value.getValue());
            }
            else {
                return new LongScalarInstant(value.getMillisFromUnix());
            }
        }

        @Override
        public ScalarValue<?> visit(KVDate value, Void arg) {
            return new LocalDateScalarDate(value.getValue());
        }

        @Override
        public ScalarValue<?> visit(KVTime value, Void arg) {
            return new LocalTimeScalarTime(value.getValue());
        }

        @Override
        public ScalarValue<?> visit(KVBinary value, Void arg) {
            return new ByteSourceScalarBinary(value.getSubtype(), value.getCategory(), value.getByteSource());
        }

        @Override
        public ScalarValue<?> visit(KVMongoTimestamp value, Void arg) {
            return new DefaultScalarMongoTimestamp(value.getSecondsSinceEpoch(), value.getOrdinal());
        }

    }

    private static class KVScalarArray extends ScalarArray {

        private static final long serialVersionUID = 6701852853081531876L;
        private final KVArray delegate;

        public KVScalarArray(KVArray delegate) {
            this.delegate = delegate;
        }

        @Override
        public Iterator<ScalarValue<?>> iterator() {
            return Iterators.transform(delegate.iterator(), AS_FUNCTION);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public ScalarValue<?> get(int index) throws IndexOutOfBoundsException {
            return fromDocValue(delegate.get(index));
        }
    }

    private static class KVScalarString extends ScalarString {

        private static final long serialVersionUID = -1736370489851107689L;
        private final KVString delegate;

        public KVScalarString(KVString delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getValue() {
            return delegate.getValue();
        }
    }
 }
