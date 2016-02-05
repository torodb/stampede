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
import com.torodb.torod.core.language.querycriteria.ExistsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TypeIsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.db.backends.query.ExistRelation;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;
import java.util.List;

/**
 *
 */
public class ExistsProcessor {

    public static List<ProcessedQueryCriteria> process(
            ExistsQueryCriteria criteria,
            QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor,
            ExistRelation existRelation) {

        List<ProcessedQueryCriteria> childResult = callChild(criteria, visitor);
        
        List<ProcessedQueryCriteria> result = Lists.newArrayListWithCapacity(childResult.size());

        for (ProcessedQueryCriteria childProcessedQueryCriteria : childResult) {

            result.add(processSingleResult(criteria, childProcessedQueryCriteria, existRelation));
        }
        return result;

    }
    
    private static List<ProcessedQueryCriteria> callChild(
            ExistsQueryCriteria criteria,
            QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor) {
        
        List<ProcessedQueryCriteria> childResult = criteria.getBody().accept(visitor, null);
        
        return childResult;
        
    }

    private static ProcessedQueryCriteria processSingleResult(
            ExistsQueryCriteria criteria, 
            ProcessedQueryCriteria childResult,
            ExistRelation existRelation) {
        
        QueryCriteria structureQuery;
        ExistsQueryCriteria dataExists;
        
        ExistsQueryCriteria structureExists = null;

        QueryCriteria arrayTypeCriteria = new TypeIsQueryCriteria(criteria.getAttributeReference(), ScalarType.ARRAY);
        if (childResult.getStructureQuery() == null) {
            structureQuery = arrayTypeCriteria;
        } else {
            structureExists = new ExistsQueryCriteria(
                    criteria.getAttributeReference(),
                    childResult.getStructureQuery()
            );
            structureQuery = new AndQueryCriteria(
                    arrayTypeCriteria,
                    structureExists
            );
        }

        if (childResult.getDataQuery() == null) {
            dataExists = null;
        } else {
            dataExists = new ExistsQueryCriteria(
                    criteria.getAttributeReference(),
                    childResult.getDataQuery());
        }
        
        if (dataExists != null && structureExists != null) {
            existRelation.addRelation(dataExists, structureExists);
        }

        return new ProcessedQueryCriteria(structureQuery, dataExists);
    }
}
