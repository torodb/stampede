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
import com.torodb.torod.core.language.AttributeReference;
import java.util.Collection;

/**
 *
 */
class MoveUpdateActionExecutor {
    
    static <K> boolean move(
            BuilderCallback<K> builder, 
            Collection<AttributeReference> originalKeys,
            AttributeReference newKeys
    ) {
        for (AttributeReference originalKey : originalKeys) {
            if (move(builder, originalKey, newKeys)) {
                return true;
            }
        }
        return false;
    }
    
    static <K> boolean move(
            BuilderCallback<K> builder, 
            AttributeReference originalKey,
            AttributeReference newKeys
    ) {
        Boolean result = AttributeReferenceToBuilderCallback.resolve(
                builder,
                originalKey.getKeys(), 
                false,
                new OriginalKeysFound(builder, newKeys)
        );
        if (result == null) {
            return false;
        }
        return result;
    }
    
    private static class OriginalKeysFound implements 
            ResolvedCallback<Boolean> {
        
        private final BuilderCallback<?> rootBuilder;
        private final AttributeReference newKeys;

        public OriginalKeysFound(
                BuilderCallback<?> rootBuilder, 
                AttributeReference newKeys
        ) {
            this.rootBuilder = rootBuilder;
            this.newKeys = newKeys;
        }

        @Override
        public <K> Boolean objectReferenced(
                BuilderCallback<K> parentBuilder, 
                K key, 
                MongoUpdatedToroDocumentBuilder child
        ) {
            parentBuilder.unset(key);
            return AttributeReferenceToBuilderCallback.resolve(
                    rootBuilder, 
                    newKeys.getKeys(), 
                    true, 
                    new MoveValueCallback(child.buildRoot())
            );
        }

        @Override
        public <K> Boolean arrayReferenced(
                BuilderCallback<K> parentBuilder, 
                K key, 
                KVArrayBuilder child
        ) {
            parentBuilder.unset(key);
            return AttributeReferenceToBuilderCallback.resolve(
                    rootBuilder, 
                    newKeys.getKeys(), 
                    true, 
                    new MoveValueCallback(child.build())
            );
        }

        @Override
        public <K> Boolean valueReferenced(
                BuilderCallback<K> parentBuilder, 
                K key, 
                KVValue child
        ) {
            parentBuilder.unset(key);
            return AttributeReferenceToBuilderCallback.resolve(
                    rootBuilder, 
                    newKeys.getKeys(), 
                    true, 
                    new MoveValueCallback(child)
            );
        }

        @Override
        public <K> Boolean newElementReferenced(
                BuilderCallback<K> parentBuilder, 
                K key
        ) {
            return false;
        }

    }
    
    private static class MoveValueCallback implements 
            ResolvedCallback<Boolean> {
        private final KVValue newValue;

        public MoveValueCallback(KVValue newValue) {
            this.newValue = newValue;
        }

        @Override
        public <K> Boolean objectReferenced(
                BuilderCallback<K> parentBuilder, 
                K key, 
                MongoUpdatedToroDocumentBuilder child
        ) {
            parentBuilder.setValue(key, newValue);
            return true;
        }

        @Override
        public <K> Boolean arrayReferenced(
                BuilderCallback<K> parentBuilder, 
                K key, 
                KVArrayBuilder child
        ) {
            parentBuilder.setValue(key, newValue);
            return true;
        }

        @Override
        public <K> Boolean valueReferenced(
                BuilderCallback<K> parentBuilder, 
                K key, 
                KVValue child
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
    }
}
