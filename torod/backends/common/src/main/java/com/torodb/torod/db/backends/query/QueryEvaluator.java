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
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.utils.AttributeReferenceResolver;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.query.dblanguage.AndDatabaseQuery;
import com.torodb.torod.db.backends.query.dblanguage.ByStructureDatabaseQuery;
import com.torodb.torod.db.backends.query.dblanguage.DatabaseQuery;
import com.torodb.torod.db.backends.query.dblanguage.DatabaseQueryVisitor;
import com.torodb.torod.db.backends.query.dblanguage.FalseDatabaseQuery;
import com.torodb.torod.db.backends.query.dblanguage.OrDatabaseQuery;
import com.torodb.torod.db.backends.query.dblanguage.SelectAllDatabaseQuery;
import com.torodb.torod.db.backends.query.dblanguage.SelectDatabaseQuery;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class QueryEvaluator {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            QueryEvaluator.class
    );
    private final IndexStorage.CollectionSchema colSchema;
    private final QueryCriteriaToSQLTranslator toSQLTranslator;
    private final DatabaseInterface databaseInterface;

    @Inject
    public QueryEvaluator(IndexStorage.CollectionSchema colSchema, DatabaseInterface databaseInterface) {
        this.colSchema = colSchema;
        this.toSQLTranslator = new QueryCriteriaToSQLTranslator(colSchema, databaseInterface);
        this.databaseInterface = databaseInterface;
    }

    /**
     * Given a {@linkplain QueryCriteria query} and a {@linkplain DSLContext SQL
     * context}, returns the document id of the documents that matchs the query.
     * <p>
     * @param criteria
     * @param dsl
     * @param maxResults 0 means no bounds. This param is ignored right now.
     * @return 
     */
    //TODO: maxResults is ignored!
    public Set<Integer> evaluateDid(
            @Nullable QueryCriteria criteria, 
            DSLContext dsl,
            int maxResults
    ) {
        DidsQueryEvaluatorCollector collector
                = new DidsQueryEvaluatorCollector();

        evaluate(criteria, dsl, collector);

        return collector.getDids();
    }

    /**
     * Given a {@linkplain QueryCriteria query} and a {@linkplain DSLContext SQL
     * context}, returns the document id of the documents that matchs the query
     * indexed by doc structure.
     * <p>
     * @param criteria
     * @param dsl
     * @return 
     */
    public Multimap<DocStructure, Integer> evaluateDidsByStructure(
            @Nullable QueryCriteria criteria, 
            DSLContext dsl
    ) {
        DidsByStructureQueryEvaluatorCollector collector
                = new DidsByStructureQueryEvaluatorCollector(colSchema.getStructuresCache());

        evaluate(criteria, dsl, collector);

        return collector.getDidsByStructure();
    }

    /**
     * Given a {@linkplain QueryCriteria query} and a {@linkplain DSLContext SQL
     * context}, returns the document id of the documents that matchs the query
     * indexed by sid.
     * <p>
     * @param criteria
     * @param dsl
     * @return 
     */
    public Multimap<Integer, Integer> evaluateDidsBySid(
            @Nullable QueryCriteria criteria, 
            DSLContext dsl
    ) {
        DidsBySidQueryEvaluatorCollector collector
                = new DidsBySidQueryEvaluatorCollector();

        evaluate(criteria, dsl, collector);

        return collector.getDidsBySid();
    }

    /**
     * Given a {@linkplain QueryCriteria query} and a {@linkplain DSLContext SQL
     * context}, collects the result in the given collector.
     * <p>
     * @param criteria
     * @param dsl
     * @param collector
     */
    public void evaluate(
            @Nullable QueryCriteria criteria, 
            DSLContext dsl, 
            QueryEvaluatorCollector collector
    ) {
        Map<Integer, DatabaseQuery> databaseQueryByStructure
                = createDatabaseQueryByStructure(criteria, dsl);

        evaluate(databaseQueryByStructure, dsl, collector);
        
        LOGGER.debug("Query {} fulfiled by {}", criteria, collector);
    }

    private void evaluate(
            Map<Integer, DatabaseQuery> databaseQueryByStructure, 
            DSLContext dsl, 
            QueryEvaluatorCollector collector
    ) {
        Evaluator evaluator = new Evaluator(colSchema, dsl);

        for (Map.Entry<Integer, DatabaseQuery> entrySet : databaseQueryByStructure.entrySet()) {
            Set<Integer> dids = entrySet.getValue().accept(evaluator, null);
            collector.addAll(entrySet.getKey(), dids);
        }
    }

    @Nonnull
    private Map<Integer, DatabaseQuery> createDatabaseQueryByStructure(
            @Nullable QueryCriteria criteria, 
            DSLContext dsl
    ) {

        Map<Integer, DatabaseQuery> result;

        if (criteria == null) {
            BiMap<Integer, DocStructure> allStructures
                    = colSchema.getStructuresCache().getAllStructures();
            result = Maps.newHashMapWithExpectedSize(allStructures.size());

            for (Integer sid : colSchema.getStructuresCache().getAllStructures().keySet()) {
                result.put(sid, SelectAllDatabaseQuery.getInstance());
            }
        }
        else {
            Multimap<Integer, QueryCriteria> candidateStructures
                    = QueryStructureFilter.filterStructures(colSchema, criteria);

            if (candidateStructures.isEmpty()) {
                result = Collections.emptyMap();
            }
            else {
                result
                        = Maps.newHashMapWithExpectedSize(candidateStructures.size());

                for (Map.Entry<Integer, QueryCriteria> entry : candidateStructures.entries()) {
                    Integer sid = entry.getKey();
                    DocStructure rootStructure
                            = colSchema.getStructuresCache().getStructure(sid);

                    DatabaseQuery databaseQuery
                            = createDatabaseQuery(entry.getValue(), sid, rootStructure, dsl);
                    if (!(databaseQuery instanceof FalseDatabaseQuery)) {
                        result.put(sid, databaseQuery);
                    }
                }
            }
        }

        return result;
    }

    @Nonnull
    private DatabaseQuery createDatabaseQuery(
            QueryCriteria criteria, 
            Integer sid, 
            DocStructure rootStructure, 
            DSLContext dsl
    ) {
        DatabaseQueryTranslator translator
                = new DatabaseQueryTranslator(sid, dsl);

        TranslatorResult translatorResult
                = criteria.accept(translator, rootStructure);

        if (translatorResult.hasStaticValue()) {
            if (translatorResult.getStaticValue()) {
                return new ByStructureDatabaseQuery(sid);
            }
            else {
                return FalseDatabaseQuery.getInstance();
            }
        }
        if (translatorResult.getDatabaseQuery() != null) {
            return translatorResult.getDatabaseQuery();
        }
        if (translatorResult.getTargetStructure() != null) {
            return createSelectDatabaseQuery(criteria, sid, translatorResult.getTargetStructure(), dsl);
        }
        if (translatorResult.isPartial()) {
            throw new AssertionError("Partial queries like " + criteria
                    + " must be part of the body of an exists "
                    + "query, but not a root query");
        }

        throw new AssertionError("Query " + criteria + " is not recognized");
    }

    private DatabaseQuery createSelectDatabaseQuery(
            QueryCriteria critera, 
            int sid, 
            DocStructure envolvedSubDocument,
            DSLContext dsl
    ) {
        return new SelectDatabaseQuery(toSQLTranslator.translate(dsl, sid, envolvedSubDocument, critera));
    }

    private class DatabaseQueryTranslator implements
            QueryCriteriaVisitor<TranslatorResult, DocStructure> {

        private final int sid;
        private final DSLContext dsl;

        public DatabaseQueryTranslator(int sid, DSLContext dsl) {
            this.sid = sid;
            this.dsl = dsl;
        }

        /**
         *
         * @param attRef
         * @param TranslatorResult
         * @param criteriaTranslatoreturn a {@link EvaluatorResult} that
         *                                contains a
         *                                {@linkplain EvaluatorResult#getTargetStructure()}
         *                                if the attribute reference can be
         *                                resolved or null if the attribute
         *                                reference is not contained in the root
         *                                structure
         * @throws UnexpectedQuery
         */
        @Nonnull
        public TranslatorResult getTargetStructure(
                AttributeReference attRef,
                DocStructure rootStructure,
                QueryCriteria criteria) throws UnexpectedQuery {

            AttributeReferenceResolver.LastDocStructureAndRelativeReference resolved
                    = AttributeReferenceResolver.resolveDocStructureElement(attRef, rootStructure);

            if (resolved == null) {
                throw new UnexpectedQuery(criteria, "The path " + attRef
                        + " is not resolvable in the given structure!");
            }
            DocStructure structure = resolved.getLastDocStructure();

            return new TranslatorResult(structure);
        }

        @Nonnull
        @Override
        public TranslatorResult visit(ContainsAttributesQueryCriteria criteria, DocStructure arg) {
            throw new UnexpectedQuery(criteria, "Queries like " + criteria
                    + " must be resolved against the structure and "
                    + "not in the database");
        }

        @Nonnull
        @Override
        public TranslatorResult visit(IsObjectQueryCriteria criteria, DocStructure arg) {
            throw new UnexpectedQuery(criteria, "Queries like " + criteria
                    + " must be resolved against the structure and "
                    + "not in the database");
        }

        @Nonnull
        @Override
        public TranslatorResult visit(TrueQueryCriteria criteria, DocStructure arg) {
            return TranslatorResult.TRUE;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(FalseQueryCriteria criteria, DocStructure arg) {
            return TranslatorResult.FALSE;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(AndQueryCriteria criteria, DocStructure arg) {
            TranslatorResult r1
                    = criteria.getSubQueryCriteria1().accept(this, arg);
            TranslatorResult r2
                    = criteria.getSubQueryCriteria2().accept(this, arg);

            if (r1.hasStaticValue()) {
                if (r1.getStaticValue()) {
                    return r2;
                }
                else {
                    return TranslatorResult.FALSE;
                }
            }

            if (r2.hasStaticValue()) {
                if (r2.getStaticValue()) {
                    return r1;
                }
                else {
                    return TranslatorResult.FALSE;
                }
            }

            DocStructure s1 = r1.getTargetStructure();
            DocStructure s2 = r2.getTargetStructure();

            if (s1 != null && s1.equals(s2)) {
                //both children have the same target structure, so the and can be executed as a single query
                return new TranslatorResult(s1);
            }

            DatabaseQuery dq1;
            DatabaseQuery dq2;

            if (s1 != null) {
                assert r1.getDatabaseQuery() == null;
                dq1
                        = createSelectDatabaseQuery(criteria.getSubQueryCriteria1(), sid, s1, dsl);
            }
            else {
                assert r1.getDatabaseQuery() != null;
                dq1 = r1.getDatabaseQuery();
            }

            if (s2 != null) {
                assert r1.getDatabaseQuery() == null;
                dq2
                        = createSelectDatabaseQuery(criteria.getSubQueryCriteria2(), sid, s2, dsl);
            }
            else {
                assert r2.getDatabaseQuery() != null;
                dq2 = r2.getDatabaseQuery();
            }

            return new TranslatorResult(new AndDatabaseQuery.Builder()
                    .add(dq1)
                    .add(dq2)
                    .build()
            );
        }

        @Nonnull
        @Override
        public TranslatorResult visit(OrQueryCriteria criteria, DocStructure arg) {
            TranslatorResult r1
                    = criteria.getSubQueryCriteria1().accept(this, arg);
            TranslatorResult r2
                    = criteria.getSubQueryCriteria2().accept(this, arg);

            if (r1.hasStaticValue()) {
                if (r1.getStaticValue()) {
                    return TranslatorResult.TRUE;
                }
                else {
                    return r2;
                }
            }

            if (r2.hasStaticValue()) {
                if (r2.getStaticValue()) {
                    return TranslatorResult.TRUE;
                }
                else {
                    return r1;
                }
            }

            DocStructure s1 = r1.getTargetStructure();
            DocStructure s2 = r2.getTargetStructure();

            if (s1 != null && s1.equals(s2)) {
                //both children have the same target structure, so the and can be executed as a single query
                return new TranslatorResult(s1);
            }

            DatabaseQuery dq1;
            DatabaseQuery dq2;

            if (s1 != null) {
                assert r1.getDatabaseQuery() == null;
                dq1
                        = createSelectDatabaseQuery(criteria.getSubQueryCriteria1(), sid, s1, dsl);
            }
            else {
                assert r1.getDatabaseQuery() != null;
                dq1 = r1.getDatabaseQuery();
            }

            if (s2 != null) {
                assert r1.getDatabaseQuery() == null;
                dq2
                        = createSelectDatabaseQuery(criteria.getSubQueryCriteria2(), sid, s2, dsl);
            }
            else {
                assert r2.getDatabaseQuery() != null;
                dq2 = r2.getDatabaseQuery();
            }

            return new TranslatorResult(new OrDatabaseQuery.Builder()
                    .add(dq1)
                    .add(dq2)
                    .build()
            );
        }

        @Nonnull
        @Override
        public TranslatorResult visit(NotQueryCriteria criteria, DocStructure arg) {
            return criteria.getSubQueryCriteria().accept(this, arg);
        }

        @Nonnull
        @Override
        public TranslatorResult visit(TypeIsQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(IsEqualQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(IsGreaterQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(IsGreaterOrEqualQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(IsLessQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Override
        public TranslatorResult visit(MatchPatternQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(IsLessOrEqualQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(InQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(ModIsQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(SizeIsQueryCriteria criteria, DocStructure arg) {
            TranslatorResult result
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return result;
        }

        @Nonnull
        @Override
        public TranslatorResult visit(ExistsQueryCriteria criteria, DocStructure arg) {
            TranslatorResult existsResult
                    = getTargetStructure(criteria.getAttributeReference(), arg, criteria);
            return existsResult;
        }

    }

    private static class TranslatorResult {

        private static final TranslatorResult TRUE
                = new TranslatorResult(Boolean.TRUE);
        private static final TranslatorResult FALSE
                = new TranslatorResult(Boolean.FALSE);

        private final DocStructure targetStructure;
        private final DatabaseQuery dataQuery;
        private final Boolean staticValue;

        /**
         * Creates a partial result.
         * <p>
         * This kind of results are part of another query and cannot be executed
         * alone. They do not have
         * {@linkplain #getStaticValue() static value}, {@linkplain #getDatabaseQuery() database query}
         * or {@linkplain #getTargetStructure() target structure}.
         */
        public TranslatorResult() {
            staticValue = null;
            dataQuery = null;
            targetStructure = null;
        }

        /**
         * Creates a result whose static value is the given.
         * <p>
         * This kind of results does not need to be executed, as they are always
         * true or false. They are not {@linkplain #isPartial() partial}, and do
         * not have {@linkplain #getDatabaseQuery() database query} or
         * {@linkplain #getTargetStructure() target structure}.
         * <p>
         * @param staticValue
         */
        private TranslatorResult(boolean staticValue) {
            this.staticValue = staticValue;
            this.dataQuery = null;
            this.targetStructure = null;
        }

        /**
         * Creates a result whose target structure.
         * <p>
         * This kind of results are not {@linkplain #isPartial() partial}, and
         * do not have {@linkplain #getStaticValue() static value} or
         * {@linkplain #getDatabaseQuery() database query}, but a database query
         * can be obtain calling {@link QueryCriteriaToSQLTranslator}
         * <p>
         * @param staticValue
         */
        public TranslatorResult(DocStructure targetStructure) {
            this.targetStructure = targetStructure;
            this.dataQuery = null;
            this.staticValue = null;
        }

        public TranslatorResult(DatabaseQuery SQLQuery) {
            this.targetStructure = null;
            this.dataQuery = SQLQuery;
            this.staticValue = null;
        }

        public boolean isPartial() {
            return staticValue == null && targetStructure == null && dataQuery
                    == null;
        }

        public boolean hasStaticValue() {
            return staticValue != null;
        }

        public boolean getStaticValue() {
            Preconditions.checkState(staticValue != null, "This result has no static value");
            return staticValue;
        }

        /**
         *
         * @return the document structure where the query should be executed.
         */
        @Nullable
        public DocStructure getTargetStructure() {
            return targetStructure;
        }

        /**
         *
         * @return the translated database query.
         */
        @Nullable
        public DatabaseQuery getDatabaseQuery() {
            return dataQuery;
        }

    }

    private static class Evaluator implements
            DatabaseQueryVisitor<Set<Integer>, Void> {

        private final IndexStorage.CollectionSchema colSchema;
        private final DSLContext dsl;

        public Evaluator(IndexStorage.CollectionSchema colSchema, DSLContext dsl) {
            this.colSchema = colSchema;
            this.dsl = dsl;
        }

        @Override
        public Set<Integer> visit(AndDatabaseQuery databaseQuery, Void argument) {
            Set<Integer> result = null;
            for (DatabaseQuery child : databaseQuery.getChildren()) {
                Set<Integer> childResult = child.accept(this, argument);
                if (result == null) {
                    result = childResult;
                }
                else {
                    result.retainAll(childResult);
                }
            }
            return result;
        }

        @Override
        public Set<Integer> visit(OrDatabaseQuery databaseQuery, Void argument) {
            Set<Integer> result = Sets.newHashSet();
            for (DatabaseQuery child : databaseQuery.getChildren()) {
                Set<Integer> childResult = child.accept(this, argument);
                result.addAll(childResult);
            }
            return result;
        }

        @Override
        public Set<Integer> visit(ByStructureDatabaseQuery databaseQuery, Void argument) {
            Result<Record1<Integer>> fetched
                    = dsl.select(DSL.field("did", Integer.class))
                    .from(DSL.tableByName(colSchema.getName(), "root"))
                    .where(
                            DSL.field("sid", Integer.class).equal(databaseQuery.getSid())
                    )
                    .fetch();

            Set<Integer> result
                    = Sets.newHashSetWithExpectedSize(fetched.size());
            for (Record1<Integer> record1 : fetched) {
                result.add(record1.value1());
            }
            return result;
        }

        @Override
        public Set<Integer> visit(FalseDatabaseQuery databaseQuery, Void argument) {
            return Collections.emptySet();
        }

        @Override
        public Set<Integer> visit(SelectDatabaseQuery databaseQuery, Void argument) {
            Result<Record1<Integer>> fetched = databaseQuery.getQuery().fetch();

            Set<Integer> result
                    = Sets.newHashSetWithExpectedSize(fetched.size());
            for (Record1<Integer> record1 : fetched) {
                result.add(record1.value1());
            }
            return result;
        }

        @Override
        public Set<Integer> visit(SelectAllDatabaseQuery databaseQuery, Void argument) {
            Result<Record1<Integer>> fetched
                    = dsl.select(DSL.field("did", Integer.class))
                    .from(DSL.tableByName(colSchema.getName(), "root"))
                    .fetch();

            Set<Integer> result
                    = Sets.newHashSetWithExpectedSize(fetched.size());
            for (Record1<Integer> record1 : fetched) {
                result.add(record1.value1());
            }
            return result;
        }

    }
}
