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

import com.torodb.torod.core.language.AttributeReference;
import com.torodb.kvdocument.values.ArrayValue;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.ObjectValue;
import java.util.Collection;

/**
 *
 */
class SetFieldUpdateActionExecutor implements ResolvedCallback<Boolean> {
    
    private final DocValue value;

    private SetFieldUpdateActionExecutor(DocValue value) {
        this.value = value;
    }
    
    static <K> boolean set(
            BuilderCallback<K> builder,
            Collection<AttributeReference> keys,
            DocValue newValue
    ) {
        for (AttributeReference key : keys) {
            if (set(builder, key, newValue)) {
                return true;
            }
        }
        return false;
    }
    
    static <K> boolean set(
            BuilderCallback<K> builder,
            AttributeReference key,
            DocValue newValue
    ) {
        Boolean result = AttributeReferenceToBuilderCallback.resolve(builder, 
                key.getKeys(), 
                true, 
                new SetFieldUpdateActionExecutor(newValue));
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
        parentBuilder.setValue(key, value);
        return true;
    }

    @Override
    public <K> Boolean arrayReferenced(
            BuilderCallback<K> parentBuilder, 
            K key, 
            ArrayValue.Builder child
    ) {
        parentBuilder.setValue(key, value);
        return true;
    }

    @Override
    public <K> Boolean valueReferenced(
            BuilderCallback<K> parentBuilder, 
            K key, 
            DocValue child
    ) {
        parentBuilder.setValue(key, value);
        return true;
    }

    @Override
    public <K> Boolean newElementReferenced(
            BuilderCallback<K> parentBuilder, 
            K key
    ) {
        parentBuilder.setValue(key, value);
        return true;
    }
}
