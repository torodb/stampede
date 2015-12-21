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

import com.torodb.torod.core.language.querycriteria.AndQueryCriteria;
import com.torodb.torod.core.language.querycriteria.AttributeAndValueQueryCriteria;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TypeIsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.ConjunctionBuilder;
import com.torodb.torod.core.language.querycriteria.utils.DisjunctionBuilder;
import com.torodb.torod.core.subdocument.BasicType;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
class Utils {

    private Utils() {
    }

    @Nullable
    static QueryCriteria getStructureQueryCriteria(AttributeAndValueQueryCriteria criteria) {
        return getStructureQueryCriteria(criteria.getAttributeReference(), criteria.getValue().getType());
    }

    @Nullable
    static QueryCriteria getStructureQueryCriteria(AttributeReference attRef, BasicType expectedType) {
        QueryCriteria refQuery = getStructureQueryCriteria(attRef);
        
        if (isTypeKnownInStructure(attRef) || expectedType.equals(BasicType.ARRAY)) {
            QueryCriteria typeQuery = new TypeIsQueryCriteria(attRef, expectedType);
            if (refQuery != null) {
                return new AndQueryCriteria(refQuery, typeQuery);
            }
            return typeQuery;
        }
        return refQuery;
    }

    /**
     * Given a collection of expected types and an {@link AttributeReference} returns the query that can be evaluated
     * agains a structure and can be used to filter structures where the given attribute reference is not one of the
     * given types.
     * <p>
     * If the given attribute has an unknown type (see 
     * {@linkplain #isTypeKnownInStructure(com.torodb.torod.core.querycriteria.AttributeReference) }), null is returned
     * (even if some of the given types, like {@linkplain BasicType#ARRAY} could be evaluated).
     * <p>
     * @param attRef
     * @param expectedTypes
     * @return a query that filter structures where the given attribute reference is not one of the given types or null
     *         if it cannot be completely evaluated.
     */
    @Nullable
    static QueryCriteria getStructureQueryCriteria(AttributeReference attRef, Collection<BasicType> expectedTypes) {
        QueryCriteria refQuery = getStructureQueryCriteria(attRef);

        if (isTypeKnownInStructure(attRef) && !expectedTypes.isEmpty()) {
            QueryCriteria typeQuery;

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (BasicType basicType : expectedTypes) {
                disjunctionBuilder.add(new TypeIsQueryCriteria(attRef, basicType));
            }
            typeQuery = disjunctionBuilder.build();

            if (refQuery == null) {
                return typeQuery;
            } else {
                return new AndQueryCriteria(refQuery, typeQuery);
            }
        }

        return refQuery;
    }

    @Nullable
    static QueryCriteria getStructureQueryCriteria(AttributeReference attRef) {
        List<AttributeReference.Key> keys = attRef.getKeys();
        if (keys.isEmpty()) {
            return null;
        }
        ConjunctionBuilder conjunctionBuilder = new ConjunctionBuilder();

        boolean restrictionsFound = false;
        for (int i = 0; i < keys.size(); i++) {
            AttributeReference.Key key = keys.get(i);

            if (key instanceof AttributeReference.ArrayKey && i > 0) {
                //if the ref starts by an array key, it does no give us new information
                conjunctionBuilder.add(
                        new TypeIsQueryCriteria(attRef.subReference(0, i), BasicType.ARRAY)
                );
                restrictionsFound = true;
            }
        }
        if (!restrictionsFound) {
            return null;
        }
        return conjunctionBuilder.build();
    }

    static boolean isTypeKnownInStructure(AttributeReference attRef) {
        List<AttributeReference.Key> keys = attRef.getKeys();
        if (keys.isEmpty()) {
            /*
             * if keys is empty, then it must be an element inside an array and it they are not resolvable if the type 
             * is not an array
             */
            return false;
        }

        AttributeReference.Key key = keys.get(keys.size() - 1);
        return key instanceof AttributeReference.ObjectKey; //iff the last key is an object key, then the type can be resolved in the subdoc type
    }

}
