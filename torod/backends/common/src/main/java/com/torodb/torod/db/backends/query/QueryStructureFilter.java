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

package com.torodb.torod.db.backends.query;

import com.torodb.torod.core.language.querycriteria.OrQueryCriteria;
import com.torodb.torod.core.language.querycriteria.ModIsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsObjectQueryCriteria;
import com.torodb.torod.core.language.querycriteria.SizeIsQueryCriteria;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.InQueryCriteria;
import com.torodb.torod.core.language.querycriteria.FalseQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsGreaterQueryCriteria;
import com.torodb.torod.core.language.querycriteria.NotQueryCriteria;
import com.torodb.torod.core.language.querycriteria.ExistsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsGreaterOrEqualQueryCriteria;
import com.torodb.torod.core.language.querycriteria.ContainsAttributesQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsLessQueryCriteria;
import com.torodb.torod.core.language.querycriteria.AndQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsLessOrEqualQueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsEqualQueryCriteria;
import com.torodb.torod.core.language.querycriteria.TypeIsQueryCriteria;
import com.google.common.collect.*;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.utils.AttributeReferenceResolver;
import com.torodb.torod.core.language.querycriteria.utils.DisjunctionBuilder;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitorAdaptor;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.core.utils.TriValuedResult;
import com.torodb.torod.db.backends.meta.StructuresCache;
import com.torodb.torod.db.backends.meta.IndexStorage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.*;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class QueryStructureFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            QueryStructureFilter.class
    );
    private static final Processor PROCESSOR = new Processor();

    public static Multimap<Integer, QueryCriteria> filterStructures(
            IndexStorage.CollectionSchema colSchema,
            QueryCriteria queryCriteria) throws UndecidableCaseException {
        return filterStructures(colSchema.getStructuresCache(), queryCriteria);
    }

    public static Multimap<Integer, QueryCriteria> filterStructures(
            StructuresCache cache,
            QueryCriteria queryCriteria) throws UndecidableCaseException {
        Processor.Result processorResult = PROCESSOR.process(queryCriteria);

        List<ProcessedQueryCriteria> processedQueryCriterias = processorResult
                .getProcessedQueryCriterias();
        ExistRelation existRelation = processorResult.getExistRelation();

        BiMap<Integer, DocStructure> allStructures = cache.getAllStructures();

        Multimap<Integer, QueryCriteria> result = HashMultimap.create(
                allStructures.size(), processedQueryCriterias.size());

        StructureQueryEvaluator structureQueryEvaluator
                = new StructureQueryEvaluator();

        for (ProcessedQueryCriteria processedQueryCriteria : processedQueryCriterias) {
            for (Map.Entry<Integer, DocStructure> entry : allStructures
                    .entrySet()) {
                if (structureConforms(structureQueryEvaluator, entry.getValue(),
                                      processedQueryCriteria.getStructureQuery())) {
                    
                    QueryCriteria dataQuery
                            = getDataQuery(
                                    processedQueryCriteria,
                                    structureQueryEvaluator
                                            .getCandidateProvider(),
                                    existRelation
                            );
                    
                    result.put(
                            entry.getKey(),
                            dataQuery
                    );
                    LOGGER.debug(
                            "Structure {} fulfil structure query {}. Data query: {}",
                            entry.getKey(),
                            processedQueryCriteria.getStructureQuery(),
                            dataQuery
                    );
                }
            }
        }

        return result;
    }

    private static boolean structureConforms(
            StructureQueryEvaluator structureQueryEvaluator,
            DocStructure structure,
            QueryCriteria query) throws UndecidableCaseException {

        if (query == null) {
            return true;
        }
        structureQueryEvaluator.setRootStructure(structure);
        return query.accept(structureQueryEvaluator, structure);
    }

    private static QueryCriteria getDataQuery(ProcessedQueryCriteria pqc,
                                              CandidateProvider candidateProvider,
                                              ExistRelation existRelation) {
        if (pqc.getDataQuery() == null) {
            return TrueQueryCriteria.getInstance();
        }
        else {
            CandidateProcesor candidateProcesor = new CandidateProcesor(
                    candidateProvider, existRelation);

            return pqc.getDataQuery().accept(candidateProcesor,
                                             new ArrayList<AttributeReference.Key>());
        }
    }

    /**
     * Evaluates a query in a structure.
     */
    private static class StructureQueryEvaluator extends QueryCriteriaVisitorAdaptor<Boolean, StructureElement> {

        private DocStructure rootStructure;
        private final CandidateProvider candidateProvider
                = new CandidateProvider();

        public void setRootStructure(DocStructure rootStructure) {
            this.rootStructure = rootStructure;
            candidateProvider.clear();
        }

        public CandidateProvider getCandidateProvider() {
            return candidateProvider;
        }

        @Override
        @Nonnull
        protected Boolean defaultCase(QueryCriteria criteria,
                                      StructureElement arg) {
            throw new IllegalArgumentException("Query " + criteria
                    + " is not valid for structure evaluator");
        }

        @Override
        @Nonnull
        public Boolean visit(ExistsQueryCriteria criteria,
                             StructureElement arg) {
            TriValuedResult<? extends StructureElement> subStructureResult
                    = AttributeReferenceResolver.resolveStructureElement(
                            criteria.getAttributeReference(),
                            arg
                    );
            if (subStructureResult.isUndecidable()) {
                throw new AssertionError("It was not expected that " 
                        + AttributeReferenceResolver.class 
                        +"#resolveStructureElement returns an undecidible value"
                );
            }
            if (subStructureResult.isNull()) {
                return false;
            }
            StructureElement subStructure = subStructureResult.getValue();

            if (!(subStructure instanceof ArrayStructure)) {
                return false;
            }
            ArrayStructure arrayStructure = (ArrayStructure) subStructure;

            QueryCriteria body = criteria.getBody();
            Boolean bestValue = false;
            for (Map.Entry<Integer, ? extends StructureElement> entry : arrayStructure
                    .getElements().entrySet()) {
                StructureElement structureElement = entry.getValue();
                Boolean accepted = body.accept(this, structureElement);
                if (accepted) {
                    candidateProvider.addCandidate(criteria, entry.getKey());
                    bestValue = true;
                    break;
                }
            }

            return bestValue;
        }

        @Override
        @Nonnull
        public Boolean visit(ContainsAttributesQueryCriteria criteria,
                             StructureElement arg) {
            TriValuedResult<? extends StructureElement> subStructureResult
                    = AttributeReferenceResolver.resolveStructureElement(
                            criteria.getAttributeReference(),
                            arg
                    );

            if (subStructureResult.isUndecidable()) {
                throw new AssertionError("It was not expected that " 
                        + AttributeReferenceResolver.class 
                        +"#resolveStructureElement returns an undecidible value"
                );
            }
            if (subStructureResult.isNull()) {
                return false;
            }
            StructureElement subStructure = subStructureResult.getValue();

            if (!(subStructure instanceof DocStructure)) {
                return false;
            }
            DocStructure docStructure = (DocStructure) subStructure;

            Set<String> keys
                    = Sets.union(
                            docStructure.getType().getAttributeKeys(),
                            docStructure.getElements().keySet()
                    );

            if (keys.size() < criteria.getAttributes().size()) {
                return false;
            }
            if (!keys.containsAll(criteria.getAttributes())) {
                return false;
            }
            return !criteria.isExclusive() || keys.size() == criteria
                    .getAttributes().size();
        }

        @Override
        @Nonnull
        public Boolean visit(IsObjectQueryCriteria criteria,
                             StructureElement arg) {
            TriValuedResult<? extends StructureElement> subStructureResult
                    = AttributeReferenceResolver.resolveStructureElement(
                            criteria.getAttributeReference(),
                            arg
                    );

            if (subStructureResult.isUndecidable()) {
                throw new AssertionError("It was not expected that " 
                        + AttributeReferenceResolver.class 
                        +"#resolveStructureElement returns an undecidible value"
                );
            }
            if (subStructureResult.isNull()) {
                return false;
            }
            StructureElement subStructure = subStructureResult.getValue();

            return subStructure instanceof DocStructure;
        }

        @Override
        @Nonnull
        public Boolean visit(TypeIsQueryCriteria criteria,
                             StructureElement arg) {
            TriValuedResult<? extends BasicType> typeResult
                    = AttributeReferenceResolver.resolveBasicType(
                            criteria.getAttributeReference(),
                            arg
                    );
            if (typeResult.isUndecidable()) {
                //it could be any scalar type, but not an array
                if (criteria.getExpectedType().equals(BasicType.ARRAY)) {
                    return false;
                }
                throw new UndecidableCaseException(criteria, rootStructure);
            }
            if (typeResult.isNull()) {
                return false;
            }

            BasicType type = typeResult.getValue();

            return type.equals(criteria.getExpectedType());
        }

        @Override
        @Nonnull
        public Boolean visit(NotQueryCriteria criteria,
                             StructureElement arg) {
            Boolean accepted = criteria.getSubQueryCriteria().accept(this, arg);

            return !accepted;
        }

        @Override
        @Nonnull
        public Boolean visit(OrQueryCriteria criteria,
                             StructureElement arg) {
            return criteria.getSubQueryCriteria1().accept(this, arg)
                    || criteria.getSubQueryCriteria2().accept(this, arg);
        }

        @Override
        @Nonnull
        public Boolean visit(AndQueryCriteria criteria,
                             StructureElement arg) {
            return criteria.getSubQueryCriteria1().accept(this, arg)
                    && criteria.getSubQueryCriteria2().accept(this, arg);
        }

        @Override
        @Nonnull
        public Boolean visit(FalseQueryCriteria criteria,
                             StructureElement arg) {
            return false;
        }

        @Override
        @Nonnull
        public Boolean visit(TrueQueryCriteria criteria,
                             StructureElement arg) {
            return true;
        }

    }

    /**
     * Given a data query and the candidates calculated by
     * {@link StructureQueryEvaluator}, returns a similar query where all
     * {@linkplain ExistsQueryCriteria exists queries} whose bodies talk about
     * objects are replaced by disjunction of the body query applied to each
     * candidate.
     */
    private static class CandidateProcesor
            implements QueryCriteriaVisitor<QueryCriteria, List<AttributeReference.Key>> {

        private final CandidateProvider candidateProvider;
        private final ExistRelation existRelation;

        public CandidateProcesor(CandidateProvider candidateProvider,
                                 ExistRelation existRelation) {
            this.candidateProvider = candidateProvider;
            this.existRelation = existRelation;
        }

        private ExistsQueryCriteria simpleExistCase(ExistsQueryCriteria criteria,
                                                    List<AttributeReference.Key> arg) {
            if (arg.isEmpty()) {
                return criteria;
            }
            return new ExistsQueryCriteria(
                    criteria.getAttributeReference().prepend(arg),
                    criteria.getBody()
            );
        }

        @Override
        public QueryCriteria visit(ExistsQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            ExistsQueryCriteria structureExist = existRelation
                    .getStructureExist(criteria);

            if (structureExist == null) {
                return simpleExistCase(criteria, arg);
            }
            Set<Integer> candidates = candidateProvider.getCandidate(
                    structureExist);
            if (candidates.isEmpty()) { // there are no candidates
                //if we reach this point, the structure matches.
                //if there is no object candidate and the structure matches => the body doesn't use objects
                return simpleExistCase(criteria, arg);
            }

            DisjunctionBuilder disjunctionBuilder = new DisjunctionBuilder();
            final int initialArgSize = arg.size();

            int attRefSize = criteria.getAttributeReference().getKeys().size();

            arg.addAll(criteria.getAttributeReference().getKeys());

            for (Integer candidate : candidates) {
                arg.add(new AttributeReference.ArrayKey(candidate));

                QueryCriteria modifiedQuery = criteria.getBody().accept(
                        this,
                        arg
                );
                disjunctionBuilder.add(modifiedQuery);

                arg.remove(arg.size() - 1);
            }

            for (int i = 0; i < attRefSize; i++) {
                arg.remove(initialArgSize);
            }

            assert arg.size() == initialArgSize;

            return disjunctionBuilder.build();
        }

        @Override
        public QueryCriteria visit(ContainsAttributesQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new ContainsAttributesQueryCriteria(
                    criteria.getAttributeReference().prepend(arg),
                    criteria.getAttributes(),
                    criteria.isExclusive()
            );
        }

        @Override
        public QueryCriteria visit(SizeIsQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new SizeIsQueryCriteria(
                    criteria.getAttributeReference().prepend(arg),
                    criteria.getValue()
            );
        }

        @Override
        public QueryCriteria visit(ModIsQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new ModIsQueryCriteria(
                    criteria.getAttributeReference().prepend(arg),
                    criteria.getDivisor(),
                    criteria.getReminder()
            );
        }

        @Override
        public QueryCriteria visit(InQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new InQueryCriteria(
                    criteria.getAttributeReference().prepend(arg),
                    criteria.getValue()
            );
        }

        @Override
        public QueryCriteria visit(IsObjectQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new IsObjectQueryCriteria(
                    criteria.getAttributeReference().prepend(arg)
            );
        }

        @Override
        public QueryCriteria visit(IsLessOrEqualQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new IsLessOrEqualQueryCriteria(criteria
                    .getAttributeReference().prepend(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(IsLessQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new IsLessQueryCriteria(criteria.getAttributeReference()
                    .prepend(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(IsGreaterOrEqualQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new IsGreaterOrEqualQueryCriteria(criteria
                    .getAttributeReference().prepend(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(IsGreaterQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new IsGreaterQueryCriteria(criteria.getAttributeReference()
                    .prepend(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(IsEqualQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new IsEqualQueryCriteria(criteria.getAttributeReference()
                    .prepend(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(MatchPatternQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new MatchPatternQueryCriteria(criteria.getAttributeReference()
                    .prepend(arg), criteria.getValue());
        }

        @Override
        public QueryCriteria visit(TypeIsQueryCriteria criteria,
                                   List<AttributeReference.Key> arg) {
            return new TypeIsQueryCriteria(criteria.getAttributeReference()
                    .prepend(arg), criteria.getExpectedType());
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
            return new AndQueryCriteria(
                    criteria.getSubQueryCriteria1().accept(this, arg),
                    criteria.getSubQueryCriteria2().accept(this, arg)
            );
        }

        @Override
        public QueryCriteria visit(OrQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new OrQueryCriteria(
                    criteria.getSubQueryCriteria1().accept(this, arg),
                    criteria.getSubQueryCriteria2().accept(this, arg)
            );
        }

        @Override
        public QueryCriteria visit(NotQueryCriteria criteria, List<AttributeReference.Key> arg) {
            return new NotQueryCriteria(
                    criteria.getSubQueryCriteria().accept(this, arg)
            );
        }

    }

    private static class CandidateProvider {

        private final Multimap<QueryCriteria, Integer> candidatesMap;

        public CandidateProvider() {
            candidatesMap = MultimapBuilder.hashKeys().hashSetValues().build();
        }

        public void addCandidate(ExistsQueryCriteria query,
                                 Integer candidate) {
            candidatesMap.put(query, candidate);
        }

        @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
        @Nonnull
        public Set<Integer> getCandidate(ExistsQueryCriteria query) {
            Set<Integer> result = (Set<Integer>) candidatesMap.get(query);
            if (result == null) {
                return Collections.emptySet();
            }
            return result;
        }

        private void clear() {
            candidatesMap.clear();
        }
    }

}
