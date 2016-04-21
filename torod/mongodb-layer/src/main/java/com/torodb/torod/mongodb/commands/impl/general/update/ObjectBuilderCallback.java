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

import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
class ObjectBuilderCallback implements BuilderCallback<String> {
    private final MongoUpdatedToroDocumentBuilder builder;

    public ObjectBuilderCallback(MongoUpdatedToroDocumentBuilder builder) {
        this.builder = builder;
    }

    @Override
    public Class<String> getKeyClass() {
        return String.class;
    }

    @Override
    public boolean contains(String key) {
        return builder.contains(key);
    }

    @Override
    public boolean isValue(String key) {
        return builder.isValue(key);
    }

    @Override
    public KVValue getValue(String key) {
        return builder.getValue(key);
    }

    @Override
    public boolean isArrayBuilder(String key) {
        return builder.isArrayBuilder(key);
    }

    @Override
    public KVArrayBuilder getArrayBuilder(String key) {
        return builder.getArrayBuilder(key);
    }

    @Override
    public boolean isObjectBuilder(String key) {
        return builder.isObjectBuilder(key);
    }

    @Override
    public MongoUpdatedToroDocumentBuilder getObjectBuilder(String key) {
        return builder.getObjectBuilder(key);
    }

    @Override
    public KVArrayBuilder newArray(String key) {
        return builder.newArray(key);
    }

    @Override
    public MongoUpdatedToroDocumentBuilder newObject(String key) {
        return builder.newObject(key);
    }

    @Override
    public void setValue(String key, KVValue value) {
        builder.putValue(key, value);
    }

    @Override
    public boolean unset(String key) {
        return builder.unset(key);
    }
    
}
