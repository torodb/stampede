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

package com.torodb.mongodb.repl;

import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.index.IndexOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.index.IndexOptions.KnownType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.repl.ReplicationFilters.IndexFieldPattern;
import com.torodb.mongodb.repl.ReplicationFilters.IndexPattern;

public class ReplicationFiltersTest {
    
    @Test
    public void unfilteredTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistDatabaseTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void blacklistDatabaseTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistAndBlacklistSameDatabaseTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistAndBlacklistTwoDatabasesTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test1"), ImmutableMap.of()));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void blacklistCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistAndBlacklistSameCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistCollectionInAllDatabasesTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void blacklistCollectionInAllDatabasesTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistCollectionInAllDatabasesAndAnotherCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of()),
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile("two"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void blacklistCollectionInAllDatabasesAndAnotherCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of()),
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile("two"), ImmutableList.of())));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistAllInADatabaseAndBlacklistCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile(".*"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistAllInADatabaseAndBlacklistSomeCollectionsTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile(".*"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile("t.*"), ImmutableList.of())));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistIndexTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), null, ImmutableList.of())))), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index1", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistIndexWithUniqueTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, ImmutableList.of())))), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", true, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index1", false, ImmutableList.of()));
    }
    
    @Test
    public void whitelistIndexWithUniqueAndFieldsTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, 
                                ImmutableList.of(new IndexFieldPattern(ImmutableList.of(Pattern.compile("subdoc"),Pattern.compile("value")), Pattern.compile("1"))))))), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertFalse(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc1", "value"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value1"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.desc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("1subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "1value"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value", "another"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("value"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index1", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    }
    
    @Test
    public void blacklistIndexTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), null, ImmutableList.of())))));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index1", false, ImmutableList.of()));
    }
    
    @Test
    public void blacklistIndexWithUniqueTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, ImmutableList.of())))));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of()));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", true, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of()));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index1", false, ImmutableList.of()));
    }
    
    @Test
    public void blacklistIndexWithUniqueAndFieldsTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"),
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, 
                                ImmutableList.of(new IndexFieldPattern(ImmutableList.of(Pattern.compile("subdoc"),Pattern.compile("value")), Pattern.compile("1"))))))));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        Assert.assertTrue(filterProvider.getDatabasePredicate().test("other"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        Assert.assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        Assert.assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc1", "value"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value1"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.desc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("1subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "1value"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value", "another"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("value"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        Assert.assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index1", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    }
}
