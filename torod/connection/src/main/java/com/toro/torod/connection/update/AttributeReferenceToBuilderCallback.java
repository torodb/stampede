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

import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
class AttributeReferenceToBuilderCallback {

    /**
     * Descends through the value tree associated to the given builder as
     * following keys and return the {@linkplain BuilderCallback callback}
     * associated with it.
     * <p>
     * @param <K>               the type of the keys of given builder
     * @param builder           the builder that will be the root of the given
     *                          keys
     * @param keys              the keys path that will be followed
     * @param createIfNecessary if true, new child values will be created if
     *                          there is no value with the given keys in the
     *                          given builder value tree. If false, null will be
     *                          returned in that case.
     * <p>
     * @throws UserToroException if keys is invalid in the given builder. That
     *                           is: if there is an index <em>i</em> where
     *                           resolve(builder, keys.subList(0,i),
     *                           createIfNecessary) is a builder of type K1 and
     *                           keys.get(i+1) is not of type K1 OR if
     *                           resolve(builder, keys.subList(0,i-1),
     *                           createIfNecessary) has a scalar value as child
     *                           indexed with keys.get(i-1)
     */
    @Nullable
    public static <R, K> R resolve(
            BuilderCallback<K> builder,
            List<AttributeReference.Key> keys,
            boolean createIfNecessary,
            ResolvedCallback<R> callback
    ) throws UserToroException {
        return resolve(builder, keys, 0, keys.size(), createIfNecessary, callback);
    }

    @Nullable
    private static <R, K> R resolve(
            BuilderCallback<K> builder,
            List<AttributeReference.Key> keys,
            int fromIndex,
            int toIndex,
            boolean createIfNecessary,
            ResolvedCallback<R> callback
    ) {
        Object uncastedKey = keys.get(fromIndex).getKeyValue();
        if (!builder.getKeyClass().isInstance(uncastedKey)) {
            throw new UserToroException(
                    "Cannot use the part ("
                    + keys.subList(fromIndex, fromIndex + 1) + " of "
                    + keys.subList(fromIndex, toIndex) + ") to transverse "
                    + "the element " + builder);
        }
        K key = builder.getKeyClass().cast(uncastedKey);

        KVArrayBuilder nextArrayBuilder;
        KVDocumentBuilder nextObjectBuilder;
        KVValue nextValue;

        if (!builder.contains(key)) {
            return nonExistingKeyCase(
                    builder,
                    keys,
                    fromIndex,
                    toIndex,
                    createIfNecessary,
                    key,
                    callback
            );
        } else {
            if (builder.isObjectBuilder(key)) {
                nextObjectBuilder = builder.getObjectBuilder(key);
                nextArrayBuilder = null;
                nextValue = null;
            } else if (builder.isArrayBuilder(key)) {
                nextObjectBuilder = null;
                nextArrayBuilder = builder.getArrayBuilder(key);
                nextValue = null;
            } else {
                nextObjectBuilder = null;
                nextArrayBuilder = null;
                nextValue = builder.getValue(key);
            }
        }

        if (fromIndex == toIndex - 1) {
            if (nextObjectBuilder != null) {
                return callback.objectReferenced(builder, key, nextObjectBuilder);
            }
            if (nextArrayBuilder != null) {
                return callback.arrayReferenced(builder, key, nextArrayBuilder);
            }
            if (nextValue != null) {
                return callback.valueReferenced(builder, key, nextValue);
            }
            throw new AssertionError();
        } else {
            BuilderCallback<?> nextBuilder = null;
            if (nextObjectBuilder != null) {
                nextBuilder = new ObjectBuilderCallback(nextObjectBuilder);
            }
            if (nextArrayBuilder != null) {
                nextBuilder = new ArrayBuilderCallback(nextArrayBuilder);
            }
            if (nextValue != null) {
                throw new UserToroException(
                        "Cannot use the part ("
                        + keys.subList(fromIndex, fromIndex + 1) + " of "
                        + keys.subList(fromIndex, toIndex) + ") to "
                        + "transverse the element " + builder);
            }
            if (nextBuilder == null) {
                throw new AssertionError();
            }
            return resolve(
                    nextBuilder,
                    keys,
                    fromIndex + 1,
                    toIndex,
                    createIfNecessary,
                    callback);
        }

    }

    @SuppressWarnings("unchecked")
    private static <R, K> R nonExistingKeyCase(
            BuilderCallback<K> builder,
            List<AttributeReference.Key> keys,
            int fromIndex,
            int toIndex,
            boolean createIfNecessary,
            K key,
            ResolvedCallback<R> callback
    ) {
        if (!createIfNecessary) {
            return null;
        }

        BuilderCallback newBuilder = builder;

        for (int i = fromIndex; i < toIndex - 1; i++) {
            AttributeReference.Key iestKey = keys.get(i);
            AttributeReference.Key nextKey = keys.get(i + 1);

            if (nextKey instanceof AttributeReference.ObjectKey) {
                newBuilder = new ObjectBuilderCallback(
                        newBuilder.newObject(iestKey.getKeyValue())
                );
            } else if (nextKey instanceof AttributeReference.ArrayKey) {
                newBuilder = new ArrayBuilderCallback(
                        newBuilder.newArray(iestKey.getKeyValue())
                );
            } else {
                throw new ToroImplementationException("Unexpected key");
            }
        }

        AttributeReference.Key lastKey = keys.get(toIndex - 1);

        if (lastKey instanceof AttributeReference.ObjectKey) {
            assert newBuilder instanceof ObjectBuilderCallback;
            
            String castedLastKey = ((AttributeReference.ObjectKey) lastKey).getKeyValue();
            
            return callback.newElementReferenced(
                    (ObjectBuilderCallback) newBuilder, 
                    castedLastKey
            );
        } else if (lastKey instanceof AttributeReference.ArrayKey) {
            assert newBuilder instanceof ArrayBuilderCallback;
            
            Integer castedLastKey = ((AttributeReference.ArrayKey) lastKey).getKeyValue();
            
            return callback.newElementReferenced(
                    (ArrayBuilderCallback) newBuilder,
                    castedLastKey
            );
        } else {
            throw new ToroImplementationException("Unexpected key");
        }
    }
}
