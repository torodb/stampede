/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.repl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.regex.Pattern;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.repl.ReplicationFilters.IndexFieldPattern;
import com.torodb.mongodb.repl.ReplicationFilters.IndexPattern;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class ReplicationFiltersTest {
	
	private static final List<IndexOptions.Key> EmptyList = ImmutableList.of();
    
    @Test
    public void unfilteredTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistDatabaseTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void blacklistDatabaseTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()));
        assertFalse(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistAndBlacklistSameDatabaseTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()));
        assertFalse(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistAndBlacklistTwoDatabasesTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of()), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test1"), ImmutableMap.of()));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void blacklistCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistAndBlacklistSameCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistCollectionInAllDatabasesTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void blacklistCollectionInAllDatabasesTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistCollectionInAllDatabasesAndAnotherCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of()),
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile("two"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void blacklistCollectionInAllDatabasesAndAnotherCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of()),
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile("two"), ImmutableList.of())));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistAllInADatabaseAndBlacklistCollectionTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile(".*"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistAllInADatabaseAndBlacklistSomeCollectionsTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile(".*"), ImmutableList.of())), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
                        Pattern.compile("test"), ImmutableMap.of(Pattern.compile("t.*"), ImmutableList.of())));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "three", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("other", "three", "index", false, EmptyList));
    }
    
    @Test
    public void whitelistIndexTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), null, ImmutableList.of())))), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index1", false, EmptyList));
    }
    
    @Test
    public void whitelistIndexWithUniqueTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, ImmutableList.of())))), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("other", "three"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", true, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index1", false, EmptyList));
    }
    
    @Test
    public void whitelistIndexWithUniqueAndFieldsTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, 
                                ImmutableList.of(new IndexFieldPattern(ImmutableList.of(Pattern.compile("subdoc"),Pattern.compile("value")), Pattern.compile("1"))))))), 
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertFalse(filterProvider.getDatabasePredicate().test("test1"));
        assertFalse(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertFalse(filterProvider.getCollectionPredicate().test("test", "two"));
        assertFalse(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc1", "value"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value1"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.desc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("1subdoc", "value"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "1value"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value", "another"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("value"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index1", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    }
    
    @Test
    public void blacklistIndexTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), null, ImmutableList.of())))));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index1", false, EmptyList));
    }
    
    @Test
    public void blacklistIndexWithUniqueTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), 
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, ImmutableList.of())))));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "three"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("other", "three"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, EmptyList));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", true, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, EmptyList));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index1", false, EmptyList));
    }
    
    @Test
    public void blacklistIndexWithUniqueAndFieldsTest() {
        ReplicationFilters filterProvider = new ReplicationFilters(
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
                ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"),
                        ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, 
                                ImmutableList.of(new IndexFieldPattern(ImmutableList.of(Pattern.compile("subdoc"),Pattern.compile("value")), Pattern.compile("1"))))))));
        assertTrue(filterProvider.getDatabasePredicate().test("test"));
        assertTrue(filterProvider.getDatabasePredicate().test("test1"));
        assertTrue(filterProvider.getDatabasePredicate().test("other"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "one"));
        assertTrue(filterProvider.getCollectionPredicate().test("test", "two"));
        assertTrue(filterProvider.getCollectionPredicate().test("test1", "one"));
        assertFalse(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc1", "value"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value1"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.desc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("1subdoc", "value"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "1value"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value", "another"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("value"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "two", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test1", "one", "index", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
        assertTrue(filterProvider.getIndexPredicate().test("test", "one", "index1", false, ImmutableList.of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    }
}
