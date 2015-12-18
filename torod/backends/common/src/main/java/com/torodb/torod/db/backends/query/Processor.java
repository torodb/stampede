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

import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.db.backends.query.processors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 */
public class Processor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
    
    public Result process(QueryCriteria query) {
        ProcessorVisitor visitor = new ProcessorVisitor();

        visitor.process(query);
        
        LOGGER.debug("Query {} translated to {}", query, visitor);
        
        return visitor;
    }

    public static interface Result {

        public ExistRelation getExistRelation();

        public List<ProcessedQueryCriteria> getProcessedQueryCriterias();
    }

    private static class ProcessorVisitor implements QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void>, Result {

        private List<ProcessedQueryCriteria> processedQueryCriterias;
        private final ExistRelation existRelation = new ExistRelation();

        public void process(QueryCriteria query) {
            processedQueryCriterias = query.accept(this, null);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(
                    100 * processedQueryCriterias.size()
            );
            for (ProcessedQueryCriteria processedQueryCriteria : processedQueryCriterias) {
                sb.append("\n\t· ").append(processedQueryCriteria.toString());
            }
            return sb.toString();
        }
        
        @Override
        public ExistRelation getExistRelation() {
            return existRelation;
        }

        @Override
        public List<ProcessedQueryCriteria> getProcessedQueryCriterias() {
            return processedQueryCriterias;
        }

        @Override
        public List<ProcessedQueryCriteria> visit(TrueQueryCriteria criteria, Void arg) {
            return TrueProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(FalseQueryCriteria criteria, Void arg) {
            return FalseProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(AndQueryCriteria criteria, Void arg) {
            return AndProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(OrQueryCriteria criteria, Void arg) {
            return OrProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(NotQueryCriteria criteria, Void arg) {
            return NotProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(TypeIsQueryCriteria criteria, Void arg) {
            return TypeIsProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(IsEqualQueryCriteria criteria, Void arg) {
            return IsEqualProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(IsGreaterQueryCriteria criteria, Void arg) {
            return CompareProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(IsGreaterOrEqualQueryCriteria criteria, Void arg) {
            return CompareProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(IsLessQueryCriteria criteria, Void arg) {
            return CompareProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(IsLessOrEqualQueryCriteria criteria, Void arg) {
            return CompareProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(InQueryCriteria criteria, Void arg) {
            return InProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(IsObjectQueryCriteria criteria, Void arg) {
            return IsObjectProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(ModIsQueryCriteria criteria, Void arg) {
            return ModIsProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(SizeIsQueryCriteria criteria, Void arg) {
            return SizeIsProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(ContainsAttributesQueryCriteria criteria, Void arg) {
            return ContainsAttributesProcessor.process(criteria, this);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(ExistsQueryCriteria criteria, Void arg) {
            return ExistsProcessor.process(criteria, this, existRelation);
        }

        @Override
        public List<ProcessedQueryCriteria> visit(MatchPatternQueryCriteria criteria, Void arg) {
            return MatchPatternProcessor.process(criteria, this);
        }
    }

}
