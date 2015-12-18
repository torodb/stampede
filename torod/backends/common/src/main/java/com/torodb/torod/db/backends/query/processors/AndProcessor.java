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

package com.torodb.torod.db.backends.query.processors;

import com.google.common.collect.Lists;
import com.torodb.torod.core.language.querycriteria.AndQueryCriteria;
import com.torodb.torod.core.language.querycriteria.OrQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;

import javax.annotation.Nullable;
import java.util.List;

/**
 *
 */
public class AndProcessor {

    public static List<ProcessedQueryCriteria> process(
            AndQueryCriteria criteria, 
            QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor) {
        
        List<ProcessedQueryCriteria> child1Result = criteria.getSubQueryCriteria1().accept(visitor, null);
        List<ProcessedQueryCriteria> child2Result = criteria.getSubQueryCriteria2().accept(visitor, null);
        
        List<ProcessedQueryCriteria> result = Lists.newArrayListWithCapacity(child1Result.size() * child2Result.size());
        for (ProcessedQueryCriteria qcr1 : child1Result) {
            for (ProcessedQueryCriteria qcr2 : child2Result) {
                
                result.add(
                        new ProcessedQueryCriteria(
                                getMergedQueryCriteria(qcr1.getStructureQuery(), qcr2.getStructureQuery()),
                                getMergedQueryCriteria(qcr1.getDataQuery(), qcr2.getDataQuery())
                        )
                );
            }
        }

        return result;
    }
    
    @Nullable
    private static QueryCriteria getMergedQueryCriteria(QueryCriteria c1, QueryCriteria c2) {
        if (c1 == null) {
            return c2;
        }
        if (c2 == null) {
            return c1;
        }
        return new AndQueryCriteria(c1, c2);
    }

    static void process(OrQueryCriteria or, QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
