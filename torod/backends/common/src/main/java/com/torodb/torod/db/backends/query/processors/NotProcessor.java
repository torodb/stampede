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
import com.torodb.torod.core.language.querycriteria.NotQueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;

import java.util.List;

/**
 *
 */
public class NotProcessor {

    public static List<ProcessedQueryCriteria> process(
            NotQueryCriteria criteria,
            QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor) {

        List<ProcessedQueryCriteria> childResults = criteria.getSubQueryCriteria().accept(visitor, null);

        List<ProcessedQueryCriteria> result = Lists.newArrayListWithCapacity(childResults.size() * 2);

        for (ProcessedQueryCriteria cr : childResults) {
            //if the structure query is not a tautology (=always true)
            //then we add the negation of the given expression with no data query
            if (cr.getStructureQuery() != null && !TrueQueryCriteria.getInstance().equals(cr.getStructureQuery())) {
                result.add(
                        new ProcessedQueryCriteria(
                                new NotQueryCriteria(cr.getStructureQuery()),
                                null
                        )
                );
            }
            if (cr.getDataQuery() != null && !TrueQueryCriteria.getInstance().equals(cr.getDataQuery())) {
                result.add(
                        new ProcessedQueryCriteria(
                                cr.getStructureQuery(),
                                new NotQueryCriteria(cr.getDataQuery()))
                );
            }
        }
        return result;
    }

}
