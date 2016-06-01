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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.torodb.torod.core.language.querycriteria.InQueryCriteria;
import com.torodb.torod.core.language.querycriteria.TypeIsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.DisjunctionBuilder;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public class InProcessor {

    public static List<ProcessedQueryCriteria> process(
            InQueryCriteria criteria,
            QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor) {

        if (!Utils.isTypeKnownInStructure(criteria.getAttributeReference())) {
            return Collections.singletonList(
                    new ProcessedQueryCriteria(null, criteria)
            );
        } else {

            Multimap<ScalarType, ScalarValue<?>> byTypeValues = MultimapBuilder
                    .enumKeys(ScalarType.class)
                    .hashSetValues()
                    .build();

            for (ScalarValue<?> value : criteria.getValue()) {
                byTypeValues.put(value.getType(), value);
            }

            List<ProcessedQueryCriteria> result;

            if (byTypeValues.isEmpty()) {
                result = Collections.emptyList();
            } else {
                result = Lists.newArrayList();

                ProcessedQueryCriteria typeQuery;

                typeQuery = getNumericQuery(criteria, byTypeValues);
                if (typeQuery != null) {
                    result.add(typeQuery);
                }

                typeQuery = getProcessedQuery(criteria, byTypeValues, ScalarType.STRING);
                if (typeQuery != null) {
                    result.add(typeQuery);
                }

                typeQuery = getProcessedQuery(criteria, byTypeValues, ScalarType.ARRAY);
                if (typeQuery != null) {
                    result.add(typeQuery);
                }

                typeQuery = getProcessedQuery(criteria, byTypeValues, ScalarType.BOOLEAN);
                if (typeQuery != null) {
                    result.add(typeQuery);
                }

                typeQuery = getProcessedQuery(criteria, byTypeValues, ScalarType.NULL);
                if (typeQuery != null) {
                    result.add(typeQuery);
                }
            }

            return result;
        }
    }

    @Nullable
    private static ProcessedQueryCriteria getNumericQuery(InQueryCriteria criteria, Multimap<ScalarType, ScalarValue<?>> byTypeValues) {
        ImmutableList.Builder<ScalarValue<?>> newInBuilder = ImmutableList.builder();

        for (ScalarValue<?> value : byTypeValues.values()) {
            newInBuilder.add(value);
        }
        
        ImmutableList<ScalarValue<?>> newIn = newInBuilder.build();

        if (newIn.isEmpty()) {
            return null;
        }
        
        DisjunctionBuilder structureBuilder = new DisjunctionBuilder();

        structureBuilder.add(new TypeIsQueryCriteria(criteria.getAttributeReference(), ScalarType.DOUBLE));
        structureBuilder.add(new TypeIsQueryCriteria(criteria.getAttributeReference(), ScalarType.INTEGER));
        structureBuilder.add(new TypeIsQueryCriteria(criteria.getAttributeReference(), ScalarType.LONG));

        newInBuilder.addAll(byTypeValues.get(ScalarType.DOUBLE));
        newInBuilder.addAll(byTypeValues.get(ScalarType.INTEGER));
        newInBuilder.addAll(byTypeValues.get(ScalarType.LONG));

        return new ProcessedQueryCriteria(structureBuilder.build(), new InQueryCriteria(criteria.getAttributeReference(), newIn));
    }

    @Nullable
    private static ProcessedQueryCriteria getProcessedQuery(InQueryCriteria criteria, Multimap<ScalarType, ScalarValue<?>> byTypeValues, ScalarType type) {
        Collection<ScalarValue<?>> values = byTypeValues.get(type);
        if (values.isEmpty()) {
            return null;
        }
        ImmutableList.Builder<ScalarValue<?>> newInBuilder = new ImmutableList.Builder();
        for (ScalarValue<?> value : values) {
            newInBuilder.add(value);
        }

        return new ProcessedQueryCriteria(
                new TypeIsQueryCriteria(
                        criteria.getAttributeReference(),
                        type
                ),
                new InQueryCriteria(
                        criteria.getAttributeReference(),
                        newInBuilder.build()
                )
        );
    }
}
