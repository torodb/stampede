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
import com.google.common.collect.Sets;
import com.torodb.torod.core.language.querycriteria.AndQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.db.backends.query.ProcessedQueryCriteria;
import org.junit.*;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 *
 */
public class AndProcessorTest {

    private AndQueryCriteria and;
    private QueryCriteria d1, d2, d3, d4, s1, s2, s3, s4, sub1, sub2;
    private QueryCriteriaVisitor<List<ProcessedQueryCriteria>, Void> visitor;

    public AndProcessorTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
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

        and = new AndQueryCriteria(sub1, sub2);

        visitor = Mockito.mock(QueryCriteriaVisitor.class);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testEmptyResult1Process() {
        List<ProcessedQueryCriteria> sub1Result = Lists.newArrayList(
                new ProcessedQueryCriteria(s1, d1), 
                new ProcessedQueryCriteria(s2, d2),
                new ProcessedQueryCriteria(null, d1),
                new ProcessedQueryCriteria(null, d2),
                new ProcessedQueryCriteria(s1, null), 
                new ProcessedQueryCriteria(s2, null),
                new ProcessedQueryCriteria(null, null)
        );
        
        List<ProcessedQueryCriteria> sub2Result = Collections.emptyList();
        
        Mockito.doReturn(sub1Result).when(sub1).accept(visitor, null);
        Mockito.doReturn(sub2Result).when(sub2).accept(visitor, null);

        HashSet<ProcessedQueryCriteria> result = Sets.newHashSet(AndProcessor.process(and, visitor));
        HashSet<ProcessedQueryCriteria> expected = Sets.newHashSet();
        
        ProcessorTestUtils.testProcessedQueryCriteriaDifference(result, expected);
    }

    @Test
    public void testEmptyResult2Process() {
        List<ProcessedQueryCriteria> sub1Result = Collections.emptyList();
        
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

        HashSet<ProcessedQueryCriteria> result = Sets.newHashSet(AndProcessor.process(and, visitor));
        HashSet<ProcessedQueryCriteria> expected = Sets.newHashSet();
        
        ProcessorTestUtils.testProcessedQueryCriteriaDifference(result, expected);
    }
    
    @Test
    public void testMultipleProcess() {
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

        HashSet<ProcessedQueryCriteria> result = Sets.newHashSet(AndProcessor.process(and, visitor));

        HashSet<ProcessedQueryCriteria> expected = Sets.newHashSet(
                new ProcessedQueryCriteria(and(s1, s3), and(d1, d3)),
                new ProcessedQueryCriteria(and(s1, s4), and(d1, d4)),
                new ProcessedQueryCriteria(and(s1, null), and(d1, d3)),
                new ProcessedQueryCriteria(and(s1, null), and(d1, d4)),
                new ProcessedQueryCriteria(and(s1, s3), and(d1, null)),
                new ProcessedQueryCriteria(and(s1, s4), and(d1, null)),
                new ProcessedQueryCriteria(and(s1, null), and(d1, null)),
                
                new ProcessedQueryCriteria(and(s2, s3), and(d2, d3)),
                new ProcessedQueryCriteria(and(s2, s4), and(d2, d4)),
                new ProcessedQueryCriteria(and(s2, null), and(d2, d3)),
                new ProcessedQueryCriteria(and(s2, null), and(d2, d4)),
                new ProcessedQueryCriteria(and(s2, s3), and(d2, null)),
                new ProcessedQueryCriteria(and(s2, s4), and(d2, null)),
                new ProcessedQueryCriteria(and(s2, null), and(d2, null)),
                
                new ProcessedQueryCriteria(and(null, s3), and(d1, d3)),
                new ProcessedQueryCriteria(and(null, s4), and(d1, d4)),
                new ProcessedQueryCriteria(and(null, null), and(d1, d3)),
                new ProcessedQueryCriteria(and(null, null), and(d1, d4)),
                new ProcessedQueryCriteria(and(null, s3), and(d1, null)),
                new ProcessedQueryCriteria(and(null, s4), and(d1, null)),
                new ProcessedQueryCriteria(and(null, null), and(d1, null)),
                
                new ProcessedQueryCriteria(and(null, s3), and(d2, d3)),
                new ProcessedQueryCriteria(and(null, s4), and(d2, d4)),
                new ProcessedQueryCriteria(and(null, null), and(d2, d3)),
                new ProcessedQueryCriteria(and(null, null), and(d2, d4)),
                new ProcessedQueryCriteria(and(null, s3), and(d2, null)),
                new ProcessedQueryCriteria(and(null, s4), and(d2, null)),
                new ProcessedQueryCriteria(and(null, null), and(d2, null)),
                
                new ProcessedQueryCriteria(and(s1, s3), and(null, d3)),
                new ProcessedQueryCriteria(and(s1, s4), and(null, d4)),
                new ProcessedQueryCriteria(and(s1, null), and(null, d3)),
                new ProcessedQueryCriteria(and(s1, null), and(null, d4)),
                new ProcessedQueryCriteria(and(s1, s3), and(null, null)),
                new ProcessedQueryCriteria(and(s1, s4), and(null, null)),
                new ProcessedQueryCriteria(and(s1, null), and(null, null)),
                
                new ProcessedQueryCriteria(and(s2, s3), and(null, d3)),
                new ProcessedQueryCriteria(and(s2, s4), and(null, d4)),
                new ProcessedQueryCriteria(and(s2, null), and(null, d3)),
                new ProcessedQueryCriteria(and(s2, null), and(null, d4)),
                new ProcessedQueryCriteria(and(s2, s3), and(null, null)),
                new ProcessedQueryCriteria(and(s2, s4), and(null, null)),
                new ProcessedQueryCriteria(and(s2, null), and(null, null)),
                
                new ProcessedQueryCriteria(and(null, s3), and(null, d3)),
                new ProcessedQueryCriteria(and(null, s4), and(null, d4)),
                new ProcessedQueryCriteria(and(null, null), and(null, d3)),
                new ProcessedQueryCriteria(and(null, null), and(null, d4)),
                new ProcessedQueryCriteria(and(null, s3), and(null, null)),
                new ProcessedQueryCriteria(and(null, s4), and(null, null)),
                new ProcessedQueryCriteria(and(null, null), and(null, null))
        );
        
        ProcessorTestUtils.testProcessedQueryCriteriaDifference(result, expected);
    }

    private static QueryCriteria and(QueryCriteria c1, QueryCriteria c2) {
        if (c1 == null) {
            return c2;
        }
        if (c2 == null) {
            return c1;
        }
        return new AndQueryCriteria(c1, c2);
    }

}
