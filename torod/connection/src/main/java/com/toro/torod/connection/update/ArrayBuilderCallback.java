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

import com.torodb.kvdocument.values.ArrayValue;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.ObjectValue;

/**
 *
 */
class ArrayBuilderCallback implements BuilderCallback<Integer> {
    private final ArrayValue.Builder builder;

    public ArrayBuilderCallback(ArrayValue.Builder builder) {
        this.builder = builder;
    }

    @Override
    public Class<Integer> getKeyClass() {
        return Integer.class;
    }

    @Override
    public boolean contains(Integer key) {
        return builder.contains(key);
    }

    @Override
    public boolean isValue(Integer key) {
        return builder.isValue(key);
    }

    @Override
    public DocValue getValue(Integer key) {
        return builder.getValue(key);
    }

    @Override
    public boolean isArrayBuilder(Integer key) {
        return builder.isArrayBuilder(key);
    }

    @Override
    public ArrayValue.Builder getArrayBuilder(Integer key) {
        return builder.getArrayBuilder(key);
    }

    @Override
    public boolean isObjectBuilder(Integer key) {
        return builder.isObjectBuilder(key);
    }

    @Override
    public ObjectValue.Builder getObjectBuilder(Integer key) {
        return builder.getObjectBuilder(key);
    }

    @Override
    public ArrayValue.Builder newArray(Integer key) {
        return builder.newArray(key);
    }

    @Override
    public ObjectValue.Builder newObject(Integer key) {
        return builder.newObject(key);
    }

    @Override
    public void setValue(Integer key, DocValue value) {
        builder.setValue(key, value);
    }

    @Override
    public boolean unset(Integer key) {
        return builder.unset(key);
    }
    
}
