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
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
public class SetFieldUpdateAction extends SingleFieldUpdateAction implements ResolvedCallback<Boolean> {

    private final KVValue<?> newValue;

    public SetFieldUpdateAction(Collection<AttributeReference> modifiedField, KVValue<?> newValue) {
        super(modifiedField);
        this.newValue = newValue;
    }

    public KVValue<?> getNewValue() {
        return newValue;
    }

    @Override
    public void apply(UpdatedToroDocumentBuilder builder) throws UpdateException {
        for (AttributeReference key : getModifiedField()) {
            if (set(new ObjectBuilderCallback(builder), key, newValue)) {
                builder.setUpdated();
                return;
            }
        }
    }

    <K> boolean set(
            BuilderCallback<K> builder,
            Collection<AttributeReference> keys,
            KVValue<?> newValue
    ) throws UpdateException {
        for (AttributeReference key : keys) {
            if (set(builder, key, newValue)) {
                return true;
            }
        }
        return false;
    }
    
    <K> boolean set(
            BuilderCallback<K> builder,
            AttributeReference key,
            KVValue<?> newValue
    ) throws UpdateException {
        Boolean result = AttributeReferenceToBuilderCallback.resolve(builder, 
                key.getKeys(), 
                true, 
                this);
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
    ) {
        parentBuilder.setValue(key, newValue);
        return true;
    }

    @Override
    public <K> Boolean arrayReferenced(
            BuilderCallback<K> parentBuilder, 
            K key, 
            UpdatedToroDocumentArrayBuilder child
    ) {
        parentBuilder.setValue(key, newValue);
        return true;
    }

    @Override
    public <K> Boolean valueReferenced(
            BuilderCallback<K> parentBuilder, 
            K key, 
            KVValue<?> child
    ) {
        parentBuilder.setValue(key, newValue);
        return true;
    }

    @Override
    public <K> Boolean newElementReferenced(
            BuilderCallback<K> parentBuilder, 
            K key
    ) {
        parentBuilder.setValue(key, newValue);
        return true;
    }

    @Override
    public <Result, Arg> Result accept(UpdateActionVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
