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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.filters.DatabaseFilter;
import com.torodb.mongodb.filters.FilterResult;
import com.torodb.mongodb.filters.IndexFilter;
import com.torodb.mongodb.filters.NamespaceFilter;
import com.torodb.mongodb.language.Namespace;
import com.torodb.mongodb.repl.filters.ReplicationFilters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UserReplicationFilters implements ReplicationFilters {

  private final ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> whitelist;
  private final ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> blacklist;

  private final DatabaseFilter dbFilter = new UserDatabaseFilter();
  private final NamespaceFilter nsFilter = new UserNamespaceFilter();
  private final IndexFilter idxFilter = new UserIndexFilter();

  public UserReplicationFilters(
      ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> whitelist,
      ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> blacklist) {
    super();
    this.whitelist = whitelist;
    this.blacklist = blacklist;


  }

  public static UserReplicationFilters allowAll() {
    return new UserReplicationFilters(ImmutableMap.of(), ImmutableMap.of());
  }

  @Override
  public DatabaseFilter getDatabaseFilter() {
    return dbFilter;
  }

  @Override
  public NamespaceFilter getNamespaceFilter() {
    return nsFilter;
  }

  @Override
  public IndexFilter getIndexFilter() {
    return idxFilter;
  }

  private boolean filterDatabase(String db) {
    return databaseWhiteFilter(db) && databaseBlackFilter(db);
  }

  private boolean filterNamespace(String db, String col) {
    return collectionWhiteFilter(db, col) && collectionBlackFilter(db, col);
  }

  private boolean filterIndex(IndexOptions indexOptions) {
    return indexWhiteFilter(indexOptions) && indexBlackFilter(indexOptions);
  }

  @SuppressWarnings("checkstyle:LineLength")
  private boolean databaseWhiteFilter(String database) {
    if (whitelist.isEmpty()) {
      return true;
    }

    for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : whitelist
        .entrySet()) {
      Matcher databaseMatcher = filterEntry.getKey().matcher(database);
      if (databaseMatcher.matches()) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings("checkstyle:LineLength")
  private boolean databaseBlackFilter(String database) {
    if (blacklist.isEmpty()) {
      return true;
    }

    for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : blacklist
        .entrySet()) {
      Matcher databaseMatcher = filterEntry.getKey().matcher(database);
      if (databaseMatcher.matches()) {
        if (filterEntry.getValue().isEmpty()) {
          return false;
        }
      }
    }

    return true;
  }

  @SuppressWarnings("checkstyle:LineLength")
  private boolean collectionWhiteFilter(String database, String collection) {
    if (whitelist.isEmpty()) {
      return true;
    }

    for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : whitelist
        .entrySet()) {
      Matcher databaseMatcher = filterEntry.getKey().matcher(database);
      if (databaseMatcher.matches()) {
        if (filterEntry.getValue().isEmpty()) {
          return true;
        }

        for (Map.Entry<Pattern, ImmutableList<IndexPattern>> collectionPattern : filterEntry
            .getValue().entrySet()) {
          Matcher collectionMatcher = collectionPattern.getKey().matcher(collection);
          if (collectionMatcher.matches()) {
            return true;
          }
        }
      }
    }

    return false;
  }

  @SuppressWarnings("checkstyle:LineLength")
  private boolean collectionBlackFilter(String database, String collection) {
    if (blacklist.isEmpty()) {
      return true;
    }

    for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : blacklist
        .entrySet()) {
      Matcher databaseMatcher = filterEntry.getKey().matcher(database);
      if (databaseMatcher.matches()) {
        if (filterEntry.getValue().isEmpty()) {
          return false;
        }

        for (Map.Entry<Pattern, ImmutableList<IndexPattern>> collectionPattern : filterEntry
            .getValue().entrySet()) {
          if (collectionPattern.getValue().isEmpty()) {
            Matcher collectionMatcher = collectionPattern.getKey().matcher(collection);
            if (collectionMatcher.matches()) {
              return false;
            }
          }
        }
      }
    }

    return true;
  }

  @SuppressWarnings("checkstyle:LineLength")
  private boolean indexWhiteFilter(IndexOptions indexOptions) {
    String database = indexOptions.getDatabase();
    String collection = indexOptions.getCollection();
    String indexName = indexOptions.getName();
    boolean unique = indexOptions.isUnique();
    List<IndexOptions.Key> keys = indexOptions.getKeys();
    if (whitelist.isEmpty()) {
      return true;
    }
    
    for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : whitelist
        .entrySet()) {
      Matcher databaseMatcher = filterEntry.getKey().matcher(database);
      if (databaseMatcher.matches()) {
        if (filterEntry.getValue().isEmpty()) {
          return true;
        }

        for (Map.Entry<Pattern, ImmutableList<IndexPattern>> collectionPattern : filterEntry
            .getValue().entrySet()) {
          Matcher collectionMatcher = collectionPattern.getKey().matcher(collection);
          if (collectionMatcher.matches()) {
            if (collectionPattern.getValue().isEmpty()) {
              return true;
            }

            for (IndexPattern indexPattern : collectionPattern.getValue()) {
              boolean match = indexPattern.match(indexName, unique, keys);
              if (match) {
                return true;
              }
            }
          }
        }
      }
    }

    return false;
  }

  @SuppressWarnings("checkstyle:LineLength")
  private boolean indexBlackFilter(IndexOptions indexOptions) {
    String database = indexOptions.getDatabase();
    String collection = indexOptions.getCollection();
    String indexName = indexOptions.getName();
    boolean unique = indexOptions.isUnique();
    List<IndexOptions.Key> keys = indexOptions.getKeys();
    if (blacklist.isEmpty()) {
      return true;
    }

    for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : blacklist
        .entrySet()) {
      Matcher databaseMatcher = filterEntry.getKey().matcher(database);
      if (databaseMatcher.matches()) {
        if (filterEntry.getValue().isEmpty()) {
          return false;
        }

        for (Map.Entry<Pattern, ImmutableList<IndexPattern>> collectionPattern : filterEntry
            .getValue().entrySet()) {
          Matcher collectionMatcher = collectionPattern.getKey().matcher(collection);
          if (collectionMatcher.matches()) {
            if (collectionPattern.getValue().isEmpty()) {
              return false;
            }

            for (IndexPattern indexPattern : collectionPattern.getValue()) {
              if (indexPattern.match(indexName, unique, keys)) {
                return false;
              }
            }
          }
        }
      }
    }

    return true;
  }

  public static class IndexPattern {

    private final Pattern name;
    private final Boolean unique;
    private final ImmutableList<IndexFieldPattern> fieldsPattern;

    public IndexPattern(@Nonnull Pattern name, @Nullable Boolean unique,
        @Nonnull ImmutableList<IndexFieldPattern> fieldsPattern) {
      super();
      this.name = name;
      this.unique = unique;
      this.fieldsPattern = fieldsPattern;
    }

    public boolean match(String name, boolean unique, List<IndexOptions.Key> fields) {
      if (this.name.matcher(name).matches() && (this.unique == null 
          || this.unique.booleanValue() == unique)
          && (this.fieldsPattern.isEmpty() || this.fieldsPattern.size() == fields.size())) {
        if (this.fieldsPattern.isEmpty()) {
          return true;
        }

        Iterator<IndexOptions.Key> fieldIterator = fields.iterator();
        Iterator<IndexFieldPattern> fieldPatternIterator = fieldsPattern.iterator();
        while (fieldPatternIterator.hasNext() && fieldIterator.hasNext()) {
          IndexFieldPattern fieldPattern = fieldPatternIterator.next();
          IndexOptions.Key field = fieldIterator.next();
          if (!fieldPattern.getType().matcher(field.getType().getName()).matches() || fieldPattern
              .getKeys().size() != field.getKeys().size()) {
            return false;
          }
          Iterator<Pattern> fieldReferencePatternIterator = fieldPattern.getKeys().iterator();
          Iterator<String> fieldReferenceIterator = field.getKeys().iterator();
          while (fieldReferencePatternIterator.hasNext() && fieldReferenceIterator.hasNext()) {
            Pattern fieldReferencePattern = fieldReferencePatternIterator.next();
            String fieldReference = fieldReferenceIterator.next();
            if (!fieldReferencePattern.matcher(fieldReference).matches()) {
              return false;
            }
          }
        }

        return true;
      }

      return false;
    }

    public static class Builder {

      private final Pattern name;
      private final Boolean unique;
      private final List<IndexFieldPattern> fieldsPattern =
          new ArrayList<>();

      public Builder(@Nonnull Pattern name, @Nullable Boolean unique) {
        this.name = name;
        this.unique = unique;
      }

      public Builder addFieldPattern(ImmutableList<Pattern> fieldReferencePattern,
          Pattern typePattern) {
        fieldsPattern.add(new IndexFieldPattern(fieldReferencePattern, typePattern));
        return this;
      }

      public IndexPattern build() {
        return new IndexPattern(name, unique, ImmutableList.copyOf(fieldsPattern));
      }
    }
  }

  public static class IndexFieldPattern {

    private final List<Pattern> keys;
    private final Pattern type;

    public IndexFieldPattern(List<Pattern> keys, Pattern type) {
      super();
      this.keys = keys;
      this.type = type;
    }

    public List<Pattern> getKeys() {
      return keys;
    }

    public Pattern getType() {
      return type;
    }
  }

  private <E> String filteredMessage(E e) {
    return e + " does not fulfill the user filter";
  }

  private class UserDatabaseFilter implements DatabaseFilter {

    @Override
    public FilterResult<String> apply(String database) {
      if (filterDatabase(database)) {
        return FilterResult.success();
      }
      return FilterResult.failure(UserReplicationFilters.this::filteredMessage);
    }

  }

  private class UserNamespaceFilter implements NamespaceFilter {

    @Override
    public FilterResult<Namespace> apply(String db, String col) {
      if (filterNamespace(db, col)) {
        return FilterResult.success();
      }
      return FilterResult.failure(UserReplicationFilters.this::filteredMessage);
    }
  }

  private class UserIndexFilter implements IndexFilter {

    @Override
    public FilterResult<IndexOptions> apply(IndexOptions indexOptions) {
      if (filterIndex(indexOptions)) {
        return FilterResult.success();
      }
      return FilterResult.failure(UserReplicationFilters.this::filteredMessage);
    }
  }
}
