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

package com.toro.torod.connection.update;

import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.kvdocument.values.ArrayValue;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.NumericDocValue;
import com.torodb.kvdocument.values.ObjectValue;
import java.util.Collection;

/**
 *
 */
class IncrementUpdateActionExecutor implements ResolvedCallback<Boolean> {

    private final NumericDocValue delta;

    public IncrementUpdateActionExecutor(NumericDocValue delta) {
        this.delta = delta;
    }

    static <K> boolean increment(
            BuilderCallback<K> builder,
            Collection<AttributeReference> keys,
            NumericDocValue delta
    ) {

        for (AttributeReference key : keys) {
            if (increment(builder, key, delta)) {
                return true;
            }
        }

        return false;
    }

    static <K> boolean increment(
            BuilderCallback<K> builder,
            AttributeReference key,
            NumericDocValue delta
    ) {
        Boolean result = AttributeReferenceToBuilderCallback.resolve(builder,
                key.getKeys(),
                true,
                new IncrementUpdateActionExecutor(delta)
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
            ObjectValue.Builder child
    ) {
        throw new UserToroException(
                "Cannot increment a value of a non-numeric type. "
                + parentBuilder + " has the field '" + key + "' of "
                + "non-numeric type object");
    }

    @Override
    public <K> Boolean arrayReferenced(
            BuilderCallback<K> parentBuilder,
            K key,
            ArrayValue.Builder child
    ) {
        throw new UserToroException(
                "Cannot increment a value of a non-numeric type. "
                + parentBuilder + " has the field '" + key + "' of "
                + "non-numeric type array");
    }

    @Override
    public <K> Boolean valueReferenced(
            BuilderCallback<K> parentBuilder,
            K key,
            DocValue child
    ) {
        if (!(child instanceof NumericDocValue)) {
            throw new UserToroException(
                    "Cannot increment a value of a non-numeric type. "
                    + parentBuilder + " has the field '" + key + "' of "
                    + "non-numeric type " + child.getType());
        }
        NumericDocValue numericChild = (NumericDocValue) child;
        parentBuilder.setValue(key, numericChild.increment(delta));
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

}
