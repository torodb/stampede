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
import com.torodb.torod.core.language.querycriteria.OrQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;


/**
 *
 */
public class OrProcessorTest {

    private OrQueryCriteria or;
    private QueryCriteria d1, d2, d3, d4, s1, s2, s3, s4, sub1, sub2;
    private QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor;

    public OrProcessorTest() {
    }

    @Before
    public void setUp() {
        d1 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("d1").when(d1).toString();
        d2 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("d2").when(d2).toString();
        d3 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("d3").when(d3).toString();
        d4 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("d4").when(d4).toString();
        s1 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("s1").when(s1).toString();
        s2 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("s2").when(s2).toString();
        s3 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("s3").when(s3).toString();
        s4 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("s4").when(s4).toString();
        sub1 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("sub1").when(sub1).toString();
        sub2 = Mockito.mock(QueryCriteria.class);
        Mockito.doReturn("sub2").when(sub2).toString();

        or = new OrQueryCriteria(sub1, sub2);

        visitor = Mockito.mock(QueryCriteriaVisitor.class);
    }

    @Test
    public void testProcess() {
        List<ProcessedQueryCriteria> sub1Result = Lists.newArrayList(
                new ProcessedQueryCriteria(s1, d1),
                new ProcessedQueryCriteria(s2, d2),
                new ProcessedQueryCriteria(null, d1),
                new ProcessedQueryCriteria(null, d2),
                new ProcessedQueryCriteria(s1, null),
                new ProcessedQueryCriteria(s2, null),
                new ProcessedQueryCriteria(null, null)
        );
        List<ProcessedQueryCriteria> sub2Result = Lists.newArrayList(
                new ProcessedQueryCriteria(s3, d3),
                new ProcessedQueryCriteria(s4, d4),
                new ProcessedQueryCriteria(null, d3),
                new ProcessedQueryCriteria(null, d4),
                new ProcessedQueryCriteria(s3, null),
                new ProcessedQueryCriteria(s4, null),
                new ProcessedQueryCriteria(null, null)
        );
        Mockito.doReturn(sub1Result).when(sub1).accept(visitor, null);
        Mockito.doReturn(sub2Result).when(sub2).accept(visitor, null);
        
        List<ProcessedQueryCriteria> result = OrProcessor.process(or, visitor);
        
        List<ProcessedQueryCriteria> expected = Lists.newArrayListWithCapacity(sub1Result.size() + sub2Result.size());
        expected.addAll(sub1Result);
        expected.addAll(sub2Result);
        
        ProcessorTestUtils.testProcessedQueryCriteriaDifference(result, expected);
        
    }

}
