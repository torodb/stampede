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

package com.torodb.packaging.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import akka.japi.function.Function5;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.language.Namespace;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.packaging.util.UserReplicationFilters.IndexFieldPattern;
import com.torodb.packaging.util.UserReplicationFilters.IndexPattern;
import org.junit.Test;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

public class UserReplicationFiltersTest {

  private static final List<IndexOptions.Key> EmptyList = ImmutableList.of();

  /**
   * Utility function to create a predicate that simplifies the test execution.
   *
   * The returned function calls {@link ReplicationFilters#getDatabaseFilter() } with the given
   * database name and maps the value to boolean.
   */
  private Function<String, Boolean> createDatabasePredicate(ReplicationFilters replFilters) {
    return (database) -> replFilters.getDatabaseFilter()
        .apply(database)
        .isSuccessful();
  }

  /**
   * Utility function to create a predicate that simplifies the test execution.
   *
   * The returned function calls {@link ReplicationFilters#getNamespaceFilter() () } with the given
   * database and collection name and maps the value to boolean.
   */
  private BiFunction<String, String, Boolean> createNamespacePredicate(ReplicationFilters replFilters) {
    return (database, collection) -> replFilters.getNamespaceFilter()
        .apply(new Namespace(database, collection))
        .isSuccessful();
  }

  /**
   * Utility function to create a predicate that simplifies the test execution.
   *
   * The returned function calls {@link ReplicationFilters#getIndexFilter() () } with the given
   * arguments name and maps the value to boolean.
   */
  private IndexPredicate createIndexPredicate(ReplicationFilters replFilters) {
    return new IndexPredicate(replFilters);
  }

  @Test
  public void unfilteredTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertTrue(nsPredicate.apply("test1", "one"));
    assertTrue(nsPredicate.apply("test1", "two"));
    assertTrue(nsPredicate.apply("test1", "three"));
    assertTrue(nsPredicate.apply("other", "one"));
    assertTrue(nsPredicate.apply("other", "two"));
    assertTrue(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistDatabaseTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of()),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void blacklistDatabaseTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of()));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertFalse(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertFalse(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertTrue(nsPredicate.apply("test1", "one"));
    assertTrue(nsPredicate.apply("test1", "two"));
    assertTrue(nsPredicate.apply("test1", "three"));
    assertTrue(nsPredicate.apply("other", "one"));
    assertTrue(nsPredicate.apply("other", "two"));
    assertTrue(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistAndBlacklistSameDatabaseTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of()),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of()));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertFalse(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertFalse(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistAndBlacklistTwoDatabasesTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of()),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test1"), ImmutableMap.of()));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistCollectionTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void blacklistCollectionTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertFalse(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertTrue(nsPredicate.apply("test1", "one"));
    assertTrue(nsPredicate.apply("test1", "two"));
    assertTrue(nsPredicate.apply("test1", "three"));
    assertTrue(nsPredicate.apply("other", "one"));
    assertTrue(nsPredicate.apply("other", "two"));
    assertTrue(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistAndBlacklistSameCollectionTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertFalse(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistCollectionInAllDatabasesTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertTrue(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertTrue(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void blacklistCollectionInAllDatabasesTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertFalse(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertTrue(nsPredicate.apply("test1", "two"));
    assertTrue(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertTrue(nsPredicate.apply("other", "two"));
    assertTrue(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistCollectionInAllDatabasesAndAnotherCollectionTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
            Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of()),
            Pattern.compile("test"), ImmutableMap.of(Pattern.compile("two"), ImmutableList.of())),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertTrue(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertTrue(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void blacklistCollectionInAllDatabasesAndAnotherCollectionTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
            Pattern.compile(".*"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of()),
            Pattern.compile("test"), ImmutableMap.of(Pattern.compile("two"), ImmutableList.of())));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertFalse(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertTrue(nsPredicate.apply("test1", "two"));
    assertTrue(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertTrue(nsPredicate.apply("other", "two"));
    assertTrue(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistAllInADatabaseAndBlacklistCollectionTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
            Pattern.compile("test"), ImmutableMap.of(Pattern.compile(".*"), ImmutableList.of())),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
            Pattern.compile("test"), ImmutableMap.of(Pattern.compile("one"), ImmutableList.of())));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertFalse(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistAllInADatabaseAndBlacklistSomeCollectionsTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
            Pattern.compile("test"), ImmutableMap.of(Pattern.compile(".*"), ImmutableList.of())),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(
            Pattern.compile("test"), ImmutableMap.of(Pattern.compile("t.*"), ImmutableList.of())));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "three", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("other", "three", "index", false, EmptyList));
  }

  @Test
  public void whitelistIndexTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"),
            ImmutableList.of(new IndexPattern(Pattern.compile("index"), null, ImmutableList.of())))),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "one", "index1", false, EmptyList));
  }

  @Test
  public void whitelistIndexWithUniqueTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"),
            ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, ImmutableList.of())))),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test", "three"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertFalse(nsPredicate.apply("test1", "two"));
    assertFalse(nsPredicate.apply("test1", "three"));
    assertFalse(nsPredicate.apply("other", "one"));
    assertFalse(nsPredicate.apply("other", "two"));
    assertFalse(nsPredicate.apply("other", "three"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", true, EmptyList));
    assertFalse(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "one", "index1", false, EmptyList));
  }

  @Test
  public void whitelistIndexWithUniqueAndFieldsTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"),
            ImmutableList.of(new IndexPattern(Pattern.compile("index"), false,
                ImmutableList.of(new IndexFieldPattern(ImmutableList.of(Pattern.compile("subdoc"),
                    Pattern.compile("value")), Pattern.compile("1"))))))),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of());

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertFalse(dbPredicate.apply("test1"));
    assertFalse(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertFalse(nsPredicate.apply("test", "two"));
    assertFalse(nsPredicate.apply("test1", "one"));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc1", "value"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value1"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.desc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("1subdoc", "value"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "1value"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value", "another"), KnownType.asc
            .getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("value"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "two", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test1", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    assertFalse(idxPredicate.apply("test", "one", "index1", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
  }

  @Test
  public void blacklistIndexTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"),
            ImmutableList.of(new IndexPattern(Pattern.compile("index"), null, ImmutableList.of())))));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertTrue(nsPredicate.apply("test1", "one"));
    assertTrue(nsPredicate.apply("test1", "two"));
    assertTrue(nsPredicate.apply("test1", "three"));
    assertTrue(nsPredicate.apply("other", "one"));
    assertTrue(nsPredicate.apply("other", "two"));
    assertTrue(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "one", "index1", false, EmptyList));
  }

  @Test
  public void blacklistIndexWithUniqueTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"),
            ImmutableList.of(new IndexPattern(Pattern.compile("index"), false, ImmutableList.of())))));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test", "three"));
    assertTrue(nsPredicate.apply("test1", "one"));
    assertTrue(nsPredicate.apply("test1", "two"));
    assertTrue(nsPredicate.apply("test1", "three"));
    assertTrue(nsPredicate.apply("other", "one"));
    assertTrue(nsPredicate.apply("other", "two"));
    assertTrue(nsPredicate.apply("other", "three"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, EmptyList));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", true, EmptyList));
    assertTrue(idxPredicate.apply("test", "two", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test1", "one", "index", false, EmptyList));
    assertTrue(idxPredicate.apply("test", "one", "index1", false, EmptyList));
  }

  @Test
  public void blacklistIndexWithUniqueAndFieldsTest() {
    ReplicationFilters filterProvider = new UserReplicationFilters(
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(),
        ImmutableMap.<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>>of(Pattern
            .compile("test"), ImmutableMap.of(Pattern.compile("one"),
            ImmutableList.of(new IndexPattern(Pattern.compile("index"), false,
                ImmutableList.of(new IndexFieldPattern(ImmutableList.of(Pattern.compile("subdoc"),
                    Pattern.compile("value")), Pattern.compile("1"))))))));

    Function<String, Boolean> dbPredicate = createDatabasePredicate(filterProvider);
    BiFunction<String, String, Boolean> nsPredicate =
        createNamespacePredicate(filterProvider);
    IndexPredicate idxPredicate = createIndexPredicate(filterProvider);

    assertTrue(dbPredicate.apply("test"));
    assertTrue(dbPredicate.apply("test1"));
    assertTrue(dbPredicate.apply("other"));
    assertTrue(nsPredicate.apply("test", "one"));
    assertTrue(nsPredicate.apply("test", "two"));
    assertTrue(nsPredicate.apply("test1", "one"));
    assertFalse(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc1", "value"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value1"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.desc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("1subdoc", "value"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "1value"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value", "another"), KnownType.asc
            .getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("value"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "two", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test1", "one", "index", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
    assertTrue(idxPredicate.apply("test", "one", "index1", false, ImmutableList
        .of(new IndexOptions.Key(ImmutableList.of("subdoc", "value"), KnownType.asc.getIndexType()))));
  }



  private static class IndexPredicate
      implements Function5<String, String, String, Boolean, List<IndexOptions.Key>, Boolean> {
    private final ReplicationFilters replFilters;

    public IndexPredicate(ReplicationFilters replFilters) {
      this.replFilters = replFilters;
    }

    @Override
    public Boolean apply(String db, String col, String indexName, Boolean unique,
        List<IndexOptions.Key> keys) {
      return replFilters.getIndexFilter().apply(new IndexOptions(
          IndexOptions.IndexVersion.V2,
          indexName,
          db,
          col,
          true,
          unique,
          false,
          0,
          keys,
          null,
          null
      )).isSuccessful();
    }
  }
}
