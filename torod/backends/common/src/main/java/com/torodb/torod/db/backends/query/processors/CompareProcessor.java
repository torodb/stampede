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
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.AttributeAndValueQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class CompareProcessor {

    public static List<ProcessedQueryCriteria> process(
            AttributeAndValueQueryCriteria criteria,
            QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor) {

        //An extra query is added in the structure part that filters all documents that does not have the given key
        //or it has incompatible type @see CompareProcessor#comparableTypes(ScalarType)
        AttributeReference attRef = criteria.getAttributeReference();
        ScalarType type = criteria.getValue().getType();

        QueryCriteria structureCriteria = Utils.getStructureQueryCriteria(attRef, comparableTypes(type));

        return Collections.singletonList(
                new ProcessedQueryCriteria(
                        structureCriteria,
                        criteria
                )
        );
    }

    private static List<ScalarType> comparableTypes(ScalarType type) {
        switch (type) {
            case DOUBLE:
            case INTEGER:
            case LONG:
                return Lists.newArrayList(ScalarType.DOUBLE, ScalarType.INTEGER, ScalarType.LONG);
            case STRING:
                return Collections.singletonList(ScalarType.STRING);
            case INSTANT:
                return Collections.singletonList(ScalarType.INSTANT);
            default:
                throw new IllegalArgumentException("Elements of type " + type + " are not comparables");
        }
    }

}
