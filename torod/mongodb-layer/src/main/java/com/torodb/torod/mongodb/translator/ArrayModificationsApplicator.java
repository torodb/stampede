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

package com.torodb.torod.mongodb.translator;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.querycriteria.utils.DisjunctionBuilder;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ArrayModificationsApplicator {

    private static final ArrayReferenceCreator ARRAY_REFERENCE_CREATOR = new ArrayReferenceCreator();
    private static final OrExistsCreator OR_EXISTS_CREATOR = new OrExistsCreator();
    private static final QueryCriteriaFactory QUERY_CRITERIA_FACTORY = new QueryCriteriaFactory();

    private ArrayModificationsApplicator() {
    }

    public static QueryCriteria translate(QueryCriteria queryCriteria) {
        QueryCriteria withArrayAlternative = queryCriteria.accept(ARRAY_REFERENCE_CREATOR, null);

        return withArrayAlternative.accept(OR_EXISTS_CREATOR, Collections.<AttributeReference.Key>emptyList());
    }

    private static class ArrayReferenceCreator implements QueryCriteriaVisitor<QueryCriteria, Void> {

        private Set<AttributeReference> combineAttributeReferences(AttributeReference attRef) {
            Set<AttributeReference> result = Sets.newHashSetWithExpectedSize(attRef.getKeys().size());

            combineAttributeReferencesRec(attRef.getKeys(), Lists.<AttributeReference.Key>newArrayList(), result);

            return result;
        }

        private void addRecursiveKey(List<AttributeReference.Key> originalRef,
                List<AttributeReference.Key> acumNewRef,
                Set<AttributeReference> acumResult,
                int index,
                AttributeReference.Key keyToAdd) {

            acumNewRef.add(keyToAdd);
            combineAttributeReferencesRec(originalRef, acumNewRef, acumResult);
            AttributeReference.Key oldKey = acumNewRef.remove(index);
            assert oldKey == keyToAdd;
        }

        private void combineAttributeReferencesRec(
                List<AttributeReference.Key> originalRef,
                List<AttributeReference.Key> acumNewRef,
                Set<AttributeReference> acumResult) {

            int index = acumNewRef.size();

            if (index != originalRef.size()) { //recursive case
                AttributeReference.Key key = originalRef.get(index);
                if (key instanceof AttributeReference.ObjectKey) {
                    AttributeReference.ObjectKey objectKey = (AttributeReference.ObjectKey) key;
                    try {
                        int keyAsInt = Integer.parseInt(objectKey.getKey());
                        AttributeReference.ArrayKey arrayKey = new AttributeReference.ArrayKey(keyAsInt);
                        addRecursiveKey(originalRef, acumNewRef, acumResult, index, arrayKey);
                    } catch (NumberFormatException ex) { //the key is not an array key candidate
                    }
                }
                addRecursiveKey(originalRef, acumNewRef, acumResult, index, key);
            } else {
                acumResult.add(new AttributeReference(acumNewRef));
            }
        }

        @Override
        public QueryCriteria visit(TrueQueryCriteria criteria, Void arg) {
            return criteria;
        }

        @Override
        public QueryCriteria visit(FalseQueryCriteria criteria, Void arg) {
            return criteria;
        }

        @Override
        public QueryCriteria visit(AndQueryCriteria criteria, Void arg) {
            QueryCriteria subQuery1 = criteria.getSubQueryCriteria1().accept(this, null);
            QueryCriteria subQuery2 = criteria.getSubQueryCriteria2().accept(this, null);

            if (subQuery1.equals(criteria.getSubQueryCriteria1()) && subQuery2.equals(criteria.getSubQueryCriteria2())) {
                return criteria;
            }

            return new AndQueryCriteria(subQuery1, subQuery2);
        }

        @Override
        public QueryCriteria visit(OrQueryCriteria criteria, Void arg) {
            QueryCriteria subQuery1 = criteria.getSubQueryCriteria1().accept(this, null);
            QueryCriteria subQuery2 = criteria.getSubQueryCriteria2().accept(this, null);

            if (subQuery1.equals(criteria.getSubQueryCriteria1()) && subQuery2.equals(criteria.getSubQueryCriteria2())) {
                return criteria;
            }

            return new OrQueryCriteria(subQuery1, subQuery2);
        }

        @Override
        public QueryCriteria visit(NotQueryCriteria criteria, Void arg) {
            QueryCriteria subQuery = criteria.getSubQueryCriteria().accept(this, null);

            if (subQuery.equals(criteria.getSubQueryCriteria())) {
                return criteria;
            }

            return new NotQueryCriteria(subQuery);
        }

        @Override
        public QueryCriteria visit(ExistsQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());

            QueryCriteria subQuery = criteria.getBody().accept(this, null);

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new ExistsQueryCriteria(
                                combinedAttRef,
                                subQuery
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(ContainsAttributesQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new ContainsAttributesQueryCriteria(
                                combinedAttRef,
                                criteria.getAttributes(),
                                criteria.isExclusive()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(SizeIsQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new SizeIsQueryCriteria(
                                combinedAttRef,
                                criteria.getValue()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(ModIsQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new ModIsQueryCriteria(
                                combinedAttRef,
                                criteria.getDivisor(),
                                criteria.getReminder()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(InQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new InQueryCriteria(
                                combinedAttRef,
                                criteria.getValue()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(IsObjectQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new IsObjectQueryCriteria(
                                combinedAttRef
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(IsLessOrEqualQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new IsLessOrEqualQueryCriteria(
                                combinedAttRef,
                                criteria.getValue()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(IsLessQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new IsLessQueryCriteria(
                                combinedAttRef,
                                criteria.getValue()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(IsGreaterOrEqualQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new IsGreaterOrEqualQueryCriteria(
                                combinedAttRef,
                                criteria.getValue()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(IsGreaterQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new IsGreaterQueryCriteria(
                                combinedAttRef,
                                criteria.getValue()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(IsEqualQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new IsEqualQueryCriteria(
                                combinedAttRef,
                                criteria.getValue()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(MatchPatternQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new MatchPatternQueryCriteria(
                                combinedAttRef,
                                criteria.getPattern()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(TypeIsQueryCriteria criteria, Void arg) {
            Set<AttributeReference> combinedAttRefs = combineAttributeReferences(criteria.getAttributeReference());
            if (combinedAttRefs.size() == 1) {
                return criteria;
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            for (AttributeReference combinedAttRef : combinedAttRefs) {
                disjunctionBuilder.add(
                        new TypeIsQueryCriteria(
                                combinedAttRef,
                                criteria.getExpectedType()
                        )
                );
            }

            return disjunctionBuilder.build();
        }

    }

    private static class OrExistsCreator implements QueryCriteriaVisitor<QueryCriteria, List<AttributeReference.Key>> {

        @Override
        public QueryCriteria visit(TrueQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return criteria;
        }

        @Override
        public QueryCriteria visit(FalseQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return criteria;
        }
        
        private QueryCriteria queryCriteriaCase(AttributeQueryCriteria criteria, List<AttributeReference.Key> arg) {
            final AttributeReference basicAttRef;
            if (arg.isEmpty()) {
                basicAttRef = criteria.getAttributeReference();
            } else {
                basicAttRef = criteria.getAttributeReference().prepend(arg);
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();

            disjunctionBuilder.add(criteria);
            
            for (int i = 0; i < basicAttRef.getKeys().size(); i++) {
                AttributeReference existsAttRef = basicAttRef.subReference(0, i +1);
                
                List<AttributeReference.Key> newArg = basicAttRef.getKeys().subList(i + 1, basicAttRef.getKeys().size());
                
                QueryCriteria plainNewBody = criteria.accept(QUERY_CRITERIA_FACTORY, newArg);
                
                QueryCriteria newBody = plainNewBody.accept(this, Collections.<AttributeReference.Key>emptyList());
                
                disjunctionBuilder.add(
                        new ExistsQueryCriteria(
                                existsAttRef,
                                newBody)
                );
            }

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(ExistsQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(AndQueryCriteria criteria, List<AttributeReference.Key> arg) {
            QueryCriteria subQuery1 = criteria.getSubQueryCriteria1().accept(this, arg);
            QueryCriteria subQuery2 = criteria.getSubQueryCriteria2().accept(this, arg);

            if (subQuery1.equals(criteria.getSubQueryCriteria1()) && subQuery2.equals(criteria.getSubQueryCriteria2())) {
                return criteria;
            }

            return new AndQueryCriteria(subQuery1, subQuery2);
        }

        @Override
        public QueryCriteria visit(OrQueryCriteria criteria, List<AttributeReference.Key> arg) {
            QueryCriteria subQuery1 = criteria.getSubQueryCriteria1().accept(this, arg);
            QueryCriteria subQuery2 = criteria.getSubQueryCriteria2().accept(this, arg);

            if (subQuery1.equals(criteria.getSubQueryCriteria1()) && subQuery2.equals(criteria.getSubQueryCriteria2())) {
                return criteria;
            }

            return new OrQueryCriteria(subQuery1, subQuery2);
        }

        @Override
        public QueryCriteria visit(NotQueryCriteria criteria, List<AttributeReference.Key> arg) {
            QueryCriteria subQuery = criteria.getSubQueryCriteria().accept(this, arg);

            if (subQuery.equals(criteria.getSubQueryCriteria())) {
                return criteria;
            }
            return new NotQueryCriteria(subQuery);
        }

        @Override
        public QueryCriteria visit(TypeIsQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(IsEqualQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(IsGreaterQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(IsGreaterOrEqualQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(IsLessQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(IsLessOrEqualQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(MatchPatternQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(IsObjectQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(InQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(ModIsQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(SizeIsQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

        @Override
        public QueryCriteria visit(ContainsAttributesQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return queryCriteriaCase(criteria, arg);
        }

    }
    
    private static class QueryCriteriaFactory implements QueryCriteriaVisitor<QueryCriteria, List<AttributeReference.Key>> {

        QueryCriteriaFactory() {
        }

        @Override
        public QueryCriteria visit(TrueQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return criteria;
        }

        @Override
        public QueryCriteria visit(FalseQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return criteria;
        }

        @Override
        public QueryCriteria visit(AndQueryCriteria criteria, List<AttributeReference.Key> arg) {
            QueryCriteria subQuery1 = criteria.getSubQueryCriteria1().accept(this, arg);
            QueryCriteria subQuery2 = criteria.getSubQueryCriteria2().accept(this, arg);

            if (subQuery1.equals(criteria.getSubQueryCriteria1()) && subQuery2.equals(criteria.getSubQueryCriteria2())) {
                return criteria;
            }

            return new AndQueryCriteria(subQuery1, subQuery2);
        }

        @Override
        public QueryCriteria visit(OrQueryCriteria criteria, List<AttributeReference.Key> arg) {
            QueryCriteria subQuery1 = criteria.getSubQueryCriteria1().accept(this, arg);
            QueryCriteria subQuery2 = criteria.getSubQueryCriteria2().accept(this, arg);

            if (subQuery1.equals(criteria.getSubQueryCriteria1()) && subQuery2.equals(criteria.getSubQueryCriteria2())) {
                return criteria;
            }

            return new OrQueryCriteria(subQuery1, subQuery2);
        }

        @Override
        public QueryCriteria visit(NotQueryCriteria criteria, List<AttributeReference.Key> arg) {
            QueryCriteria subQuery = criteria.getSubQueryCriteria().accept(this, arg);

            if (subQuery.equals(criteria.getSubQueryCriteria())) {
                return criteria;
            }
            return new NotQueryCriteria(subQuery);
        }

        @Override
        public QueryCriteria visit(TypeIsQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new TypeIsQueryCriteria(new AttributeReference(arg), criteria.getExpectedType());
        }

        @Override
        public QueryCriteria visit(IsEqualQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new IsEqualQueryCriteria(new AttributeReference(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(IsGreaterQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new IsGreaterQueryCriteria(new AttributeReference(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(IsGreaterOrEqualQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new IsGreaterOrEqualQueryCriteria(new AttributeReference(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(IsLessQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new IsLessQueryCriteria(new AttributeReference(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(IsLessOrEqualQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new IsLessOrEqualQueryCriteria(new AttributeReference(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(MatchPatternQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new MatchPatternQueryCriteria(new AttributeReference(arg), criteria.getPattern());
        }

        @Override
        public QueryCriteria visit(IsObjectQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new IsObjectQueryCriteria(new AttributeReference(arg));
        }

        @Override
        public QueryCriteria visit(InQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new InQueryCriteria(new AttributeReference(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(ModIsQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new ModIsQueryCriteria(new AttributeReference(arg), criteria.getDivisor(), criteria.getReminder());
        }

        @Override
        public QueryCriteria visit(SizeIsQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new SizeIsQueryCriteria(new AttributeReference(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(ContainsAttributesQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new ContainsAttributesQueryCriteria(new AttributeReference(arg), criteria.getAttributes(), criteria.isExclusive());
        }

        @Override
        public QueryCriteria visit(ExistsQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new ExistsQueryCriteria(new AttributeReference(arg), criteria.getBody());
        }
        
    }
}
