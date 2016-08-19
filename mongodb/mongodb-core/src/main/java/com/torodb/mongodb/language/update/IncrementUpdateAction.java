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

package com.torodb.mongodb.language.update;

import java.util.Collection;

import com.torodb.core.exceptions.user.UpdateException;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVNumeric;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.KVValueAdaptor;

/**
 *
 */
public class IncrementUpdateAction extends SingleFieldUpdateAction implements ResolvedCallback<Boolean> {

    private static final Incrementer INCREMENTER = new Incrementer();
    
    private final KVNumeric<?> delta;

    public IncrementUpdateAction(Collection<AttributeReference> modifiedField, KVNumeric<?> delta) {
        super(modifiedField);
        this.delta = delta;
    }

    public KVNumeric<?> getDelta() {
        return delta;
    }

    @Override
    public void apply(UpdatedToroDocumentBuilder builder) throws UpdateException {
        for (AttributeReference key : getModifiedField()) {
            if (increment(new ObjectBuilderCallback(builder), key, delta)) {
                builder.setUpdated();
                return;
            }
        }
    }

    <K> boolean increment(
            BuilderCallback<K> builder,
            AttributeReference key,
            KVNumeric<?> delta
    ) throws UpdateException {
        Boolean result = AttributeReferenceToBuilderCallback.resolve(builder,
                key.getKeys(),
                true,
                this
        );
        if (result == null) {
            return false;
        }
        return result;
    }

    @Override
    public <K> Boolean objectReferenced(
            BuilderCallback<K> parentBuilder,
            K key,
            UpdatedToroDocumentBuilder child
    ) throws UpdateException {
        throw new UpdateException(
                "Cannot increment a value of a non-numeric type. "
                + parentBuilder + " has the field '" + key + "' of "
                + "non-numeric type object");
    }

    @Override
    public <K> Boolean arrayReferenced(
            BuilderCallback<K> parentBuilder,
            K key,
            UpdatedToroDocumentArrayBuilder child
    ) throws UpdateException {
        throw new UpdateException(
                "Cannot increment a value of a non-numeric type. "
                + parentBuilder + " has the field '" + key + "' of "
                + "non-numeric type array");
    }

    @Override
    public <K> Boolean valueReferenced(
            BuilderCallback<K> parentBuilder,
            K key,
            KVValue<?> child
    ) throws UpdateException {
        if (!(child instanceof KVNumeric)) {
            throw new UpdateException(
                    "Cannot increment a value of a non-numeric type. "
                    + parentBuilder + " has the field '" + key + "' of "
                    + "non-numeric type " + child.getType());
        }
        KVNumeric<?> numericChild = (KVNumeric<?>) child;
        parentBuilder.setValue(key, numericChild.accept(INCREMENTER, delta));
        return true;
    }

    @Override
    public <K> Boolean newElementReferenced(
            BuilderCallback<K> parentBuilder,
            K key
    ) {
        parentBuilder.setValue(key, delta);
        return true;
    }

    @Override
    public <Result, Arg> Result accept(UpdateActionVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class Incrementer extends KVValueAdaptor<KVNumeric<?>, KVNumeric<?>> {

        @Override
        public KVNumeric<?> visit(KVDouble value, KVNumeric<?> arg) {
            return KVDouble.of(value.doubleValue() + arg.doubleValue());
        }

        @Override
        public KVNumeric<?> visit(KVLong value, KVNumeric<?> arg) {
            return KVDouble.of(value.doubleValue() + arg.doubleValue());
        }

        @Override
        public KVNumeric<?> visit(KVInteger value, KVNumeric<?> arg) {
            return KVDouble.of(value.doubleValue() + arg.doubleValue());
        }

        @Override
        public KVNumeric<?> defaultCase(KVValue<?> value, KVNumeric<?> arg) {
            throw new AssertionError("Trying to increment a value of type " + value.getType() + " which is not a number");
        }

    }
    
}
