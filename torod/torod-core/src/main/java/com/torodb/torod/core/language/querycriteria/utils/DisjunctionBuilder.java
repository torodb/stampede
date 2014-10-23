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

import com.torodb.torod.core.language.querycriteria.OrQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;

/**
 *
 */
public class DisjunctionBuilder extends LogicalBinaryQueryCriteriaBuilder<OrQueryCriteria> {

    @Override
    QueryCriteria concat(QueryCriteria elem1, QueryCriteria elem2) {
        return new OrQueryCriteria(elem1, elem2);
    }

    @Override
    public DisjunctionBuilder add(QueryCriteria query) {
        return (DisjunctionBuilder) super.add(query);
    }

    @Override
    public DisjunctionBuilder addAll(Iterable<? extends QueryCriteria> queryCriterias) {
        return (DisjunctionBuilder) super.addAll(queryCriterias);
    }
    
}
