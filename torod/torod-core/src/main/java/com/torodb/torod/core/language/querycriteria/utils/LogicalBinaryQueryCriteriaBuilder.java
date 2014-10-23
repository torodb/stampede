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


package com.torodb.torod.core.language.querycriteria.utils;

import com.google.common.base.Preconditions;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import javax.annotation.Nullable;

/**
 *
 */
abstract class LogicalBinaryQueryCriteriaBuilder<Q extends QueryCriteria> {
    private QueryCriteria root;

    abstract QueryCriteria concat(QueryCriteria elem1, QueryCriteria elem2);

    LogicalBinaryQueryCriteriaBuilder<Q> add(@Nullable QueryCriteria query) {
        if (query == null) {
            return this;
        }
        if (root == null) {
            root = query;
        } else {
            root = concat(root, query);
        }
        return this;
    }
    
    LogicalBinaryQueryCriteriaBuilder<Q> addAll(Iterable<? extends QueryCriteria> queryCriterias) {
        for (QueryCriteria queryCriteria : queryCriterias) {
            add(queryCriteria);
        }
        return this;
    }
    
    public boolean isEmpty() {
        return root == null;
    }
    
    public void reset() {
        root = null;
    }

    public QueryCriteria build() {
        Preconditions.checkState(root != null, "At least one query criteria must be added");
        return root;
    }
    
}
