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


package com.torodb.torod.db.backends.metaInf.idHeuristic;

import com.torodb.torod.db.backends.metaInf.ReservedIdHeuristic;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.Nonnegative;
import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;
import javax.inject.Qualifier;

/**
 *
 */
@Immutable
public class PoolReserveIdHeuristic implements ReservedIdHeuristic {
    private final int pool;
    private final int limit;

    @Inject
    public PoolReserveIdHeuristic(@Nonnegative @PoolSize int pool, @Nonnegative @LoadFactor double loadFactor) {
        if (pool < 0) {
            throw new IllegalArgumentException("The pool must not be negative");
        }
        this.pool = pool;
        if (loadFactor <= 0 || loadFactor >= 1) {
            throw new IllegalArgumentException("The load factor must be in (0..1)");
        }
        this.limit = (int) Math.round(loadFactor * pool);
    }

    @Override
    public int evaluate(int usedId, int cachedId) {
        int freeCached = cachedId - usedId;
        if (freeCached >= limit) {
            return 0;
        }
        if (freeCached < 0) {
            return pool - freeCached;
        }
        return pool;
    }
    
    @Qualifier
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface PoolSize {}
    
    @Qualifier
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface LoadFactor {}
    
}
