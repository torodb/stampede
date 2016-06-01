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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class ProcessorTestUtils {

    private ProcessorTestUtils() {
    }

    public static void testQueryCriteriaDifference(Set<QueryCriteria> result, Set<QueryCriteria> expected) {
        Set<QueryCriteriaWrapper> difference;

        difference = convertQueryCriteria(result);
        difference.removeAll(convertQueryCriteria(expected));
        assert difference.isEmpty() : "The following queries were expected but not contained "
                + "in the result: " + difference
                + "\n\tExpected: " + expected + "\n\tResult:   " + result;

        difference = convertQueryCriteria(expected);
        difference.removeAll(convertQueryCriteria(result));
        assert difference.isEmpty() : difference + " expected elements has not been processed";
        
        assert difference.isEmpty() : "The following queries were returned but not expected: "
                + "in the result: " + difference
                + "\n\tExpected: " + expected + "\n\tResult:   " + result;
    }
    
    public static void testProcessedQueryCriteriaDifference(HashSet<ProcessedQueryCriteria> result, HashSet<ProcessedQueryCriteria> expected) {
        Set<ProcessedQueryCriteriaWrapper> difference;

        difference = convertProcessedQueryCriteria(result);
        difference.removeAll(convertProcessedQueryCriteria(expected));
        assert difference.isEmpty() : difference + " unexpected elements has been processed";

        difference = convertProcessedQueryCriteria(expected);
        difference.removeAll(convertProcessedQueryCriteria(result));
        assert difference.isEmpty() : difference + " expected elements has not been processed";
    }

    public static void testProcessedQueryCriteriaDifference(Collection<ProcessedQueryCriteria> result, Collection<ProcessedQueryCriteria> expected) {
        HashSet<ProcessedQueryCriteria> resultSet = Sets.newHashSet(result);
        HashSet<ProcessedQueryCriteria> expectedSet = Sets.newHashSet(expected);

        ProcessorTestUtils.testProcessedQueryCriteriaDifference(resultSet, expectedSet);
    }

    private static HashSet<ProcessedQueryCriteriaWrapper> convertProcessedQueryCriteria(Set<ProcessedQueryCriteria> queries) {
        HashSet<ProcessedQueryCriteriaWrapper> result = Sets.newHashSetWithExpectedSize(queries.size());

        Iterables.addAll(result,
                Iterables.transform(
                        queries,
                        new Function<ProcessedQueryCriteria, ProcessedQueryCriteriaWrapper>() {

                            @Override
                            public ProcessedQueryCriteriaWrapper apply(ProcessedQueryCriteria input) {
                                return new ProcessedQueryCriteriaWrapper(input);
                            }
                        }
                )
        );

        return result;
    }

    private static HashSet<QueryCriteriaWrapper> convertQueryCriteria(Set<QueryCriteria> queries) {
        HashSet<QueryCriteriaWrapper> result = Sets.newHashSetWithExpectedSize(queries.size());

        Iterables.addAll(result,
                Iterables.transform(
                        queries,
                        new Function<QueryCriteria, QueryCriteriaWrapper>() {

                            @Override
                            public QueryCriteriaWrapper apply(QueryCriteria input) {
                                return new QueryCriteriaWrapper(input);
                            }
                        }
                )
        );

        return result;
    }
    

    private static class ProcessedQueryCriteriaWrapper {

        private final ProcessedQueryCriteria wrapped;

        public ProcessedQueryCriteriaWrapper(ProcessedQueryCriteria wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 29 * hash + (this.wrapped != null ? this.wrapped.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ProcessedQueryCriteriaWrapper other = (ProcessedQueryCriteriaWrapper) obj;
            return !(this.wrapped != other.wrapped && (this.wrapped == null || !this.wrapped.semanticEquals(other.wrapped)));
        }

        @Override
        public String toString() {
            return wrapped.toString();
        }
    }
    
    private static class QueryCriteriaWrapper {

        private final QueryCriteria wrapped;

        public QueryCriteriaWrapper(QueryCriteria wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 29 * hash + (this.wrapped != null ? this.wrapped.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final QueryCriteriaWrapper other = (QueryCriteriaWrapper) obj;
            return !(this.wrapped != other.wrapped && (this.wrapped == null || !this.wrapped.semanticEquals(other.wrapped)));
        }

        @Override
        public String toString() {
            return wrapped.toString();
        }
    }
}
