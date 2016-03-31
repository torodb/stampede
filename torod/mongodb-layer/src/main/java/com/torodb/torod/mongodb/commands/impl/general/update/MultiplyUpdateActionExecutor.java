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

package com.torodb.torod.mongodb.commands.impl.general.update;

import com.torodb.kvdocument.values.*;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import java.util.Collection;


/**
 *
 */
class MultiplyUpdateActionExecutor implements ResolvedCallback<Boolean> {

    private static final Multiplicator MULTIPLICATOR = new Multiplicator();
    private final KVNumeric<?> multiplier;

    private MultiplyUpdateActionExecutor(KVNumeric<?> multiplier) {
        this.multiplier = multiplier;
    }

    static <K> boolean multiply(
            BuilderCallback<K> builder,
            Collection<AttributeReference> keys,
            KVNumeric<?> multiplier
    ) {
        for (AttributeReference key : keys) {
            if (multiply(builder, key, multiplier)) {
                return true;
            }
        }
        return false;
    }

    static <K> boolean multiply(
            BuilderCallback<K> builder,
            AttributeReference key,
            KVNumeric<?> multiplier
    ) {
        Boolean result = AttributeReferenceToBuilderCallback.resolve(
                builder,
                key.getKeys(),
                true,
                new MultiplyUpdateActionExecutor(multiplier)
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
            MongoUpdatedToroDocumentBuilder child
    ) {
        throw new UserToroException(
                "Cannot multiply a value of a non-numeric type. "
                + parentBuilder + " has the field '" + key + "' of "
                + "non-numeric type object");
    }

    @Override
    public <K> Boolean arrayReferenced(
            BuilderCallback<K> parentBuilder,
            K key,
            KVArrayBuilder child
    ) {
        throw new UserToroException(
                "Cannot multiply a value of a non-numeric type. "
                + parentBuilder + " has the field '" + key + "' of "
                + "non-numeric type array");
    }

    @Override
    public <K> Boolean valueReferenced(
            BuilderCallback<K> parentBuilder,
            K key,
            KVValue child
    ) {
        if (!(child instanceof KVNumeric)) {
            throw new UserToroException(
                    "Cannot increment a value of a non-numeric type. "
                    + parentBuilder + " has the field '" + key + "' of "
                    + "non-numeric type " + child.getType());
        }
        KVNumeric<?> numericChild = (KVNumeric<?>) child;
        parentBuilder.setValue(key, numericChild.accept(MULTIPLICATOR, multiplier));
        return true;
    }

    @Override
    public <K> Boolean newElementReferenced(
            BuilderCallback<K> parentBuilder,
            K key
    ) {
        parentBuilder.setValue(key, multiplier.accept(MULTIPLICATOR, KVInteger.of(0)));
        return true;
    }

    private static class Multiplicator extends KVValueAdaptor<KVNumeric<?>, KVNumeric<?>> {

        @Override
        public KVNumeric<?> visit(KVDouble value, KVNumeric<?> arg) {
            return KVDouble.of(value.doubleValue() * arg.doubleValue());
        }

        @Override
        public KVNumeric<?> visit(KVLong value, KVNumeric<?> arg) {
            return KVDouble.of(value.doubleValue() * arg.doubleValue());
        }

        @Override
        public KVNumeric<?> visit(KVInteger value, KVNumeric<?> arg) {
            return KVDouble.of(value.doubleValue() * arg.doubleValue());
        }

        @Override
        public KVNumeric<?> defaultCase(KVValue<?> value, KVNumeric<?> arg) {
            throw new AssertionError("Trying to multiply a value of type " + value.getType() + " which is not a number");
        }

    }

}
