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
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.ExistsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TypeIsQueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.db.backends.query.ExistRelation;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 */
public class ExistsProcessorTest {

    private AttributeReference attRef;
    private ExistsQueryCriteria exists;
    private QueryCriteria body;
    private QueryCriteria d1;
    private QueryCriteria s1;
    private QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor;

    public ExistsProcessorTest() {
    }

    @Before
    public void setUp() {
        body = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("body").when(body).toString();

        attRef = Mockito.mock(AttributeReference.class);
        Mockito.doReturn("attRef").when(attRef).toString();

        exists = Mockito.mock(ExistsQueryCriteria.class);
        Mockito.doReturn(body).when(exists).getBody();
        Mockito.doReturn(attRef).when(exists).getAttributeReference();

        d1 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("d1").when(d1).toString();

        s1 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("s1").when(s1).toString();

        visitor = Mockito.mock(QueryCriteriaVisitor.class);
    }

    @Test
    public void testProcessNullNull() {
        final List<ProcessedQueryCriteria> childResult = Lists.newArrayList(
                new ProcessedQueryCriteria(null, null)
        );

        Mockito.doReturn(childResult).when(body).accept(visitor, null);

        List<ProcessedQueryCriteria> expected = Lists.newArrayList(
                new ProcessedQueryCriteria(
                        new TypeIsQueryCriteria(attRef, BasicType.ARRAY),
                        null
                )
        );

        ExistRelation existRelation = new ExistRelation();
        
        List<ProcessedQueryCriteria> result = ExistsProcessor.process(exists, visitor, existRelation);
        
        assert existRelation.isEmpty();
        
        ProcessorTestUtils.testProcessedQueryCriteriaDifference(result, expected);
    }

    @Test
    public void testProcessNotNullNull() {
        List<ProcessedQueryCriteria> childResult = Lists.newArrayList(
                new ProcessedQueryCriteria(s1, null)
        );

        Mockito.doReturn(childResult).when(body).accept(visitor, null);

        List<ProcessedQueryCriteria> expected = Lists.newArrayList(
                new ProcessedQueryCriteria(
                        new AndQueryCriteria(
                                new TypeIsQueryCriteria(attRef, BasicType.ARRAY),
                                new ExistsQueryCriteria(
                                        attRef,
                                        s1
                                )
                        ),
                        null
                )
        
        );
        
        ExistRelation existRelation = new ExistRelation();
        
        List<ProcessedQueryCriteria> result = ExistsProcessor.process(exists, visitor, existRelation);
        
        assert existRelation.isEmpty();

        ProcessorTestUtils.testProcessedQueryCriteriaDifference(result, expected);
    }

    @Test
    public void testProcessNullNotNull() {
        List<ProcessedQueryCriteria> childResult = Lists.newArrayList(
                new ProcessedQueryCriteria(null, d1)
        );

        Mockito.doReturn(childResult).when(body).accept(visitor, null);

        List<ProcessedQueryCriteria> expected = Lists.newArrayList(
                new ProcessedQueryCriteria(
                        new TypeIsQueryCriteria(attRef, BasicType.ARRAY),
                        new ExistsQueryCriteria(attRef, d1)
                )
        );
        
        ExistRelation existRelation = new ExistRelation();
        
        List<ProcessedQueryCriteria> result = ExistsProcessor.process(exists, visitor, existRelation);
        
        assert existRelation.isEmpty();

        ProcessorTestUtils.testProcessedQueryCriteriaDifference(result, expected);
    }

    @Test
    public void testProcessNotNullNotNull() {
        List<ProcessedQueryCriteria> childResult = Lists.newArrayList(
                new ProcessedQueryCriteria(s1, d1)
        );

        Mockito.doReturn(childResult).when(body).accept(visitor, null);

        List<ProcessedQueryCriteria> expected = Lists.newArrayList(
                new ProcessedQueryCriteria(
                        new AndQueryCriteria(
                                new TypeIsQueryCriteria(attRef, BasicType.ARRAY),
                                new ExistsQueryCriteria(
                                        attRef,
                                        s1
                                )
                        ),
                        new ExistsQueryCriteria(attRef, d1)
                )
        );
        
        ExistRelation existRelation = new ExistRelation();
        
        List<ProcessedQueryCriteria> result = ExistsProcessor.process(exists, visitor, existRelation);
        
        assert !existRelation.isEmpty();

        ProcessorTestUtils.testProcessedQueryCriteriaDifference(result, expected);
    }
}
