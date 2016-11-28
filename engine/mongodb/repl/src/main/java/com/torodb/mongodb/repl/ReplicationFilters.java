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

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.oplog.DbCmdOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DbOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DeleteOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.NoopOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperationVisitor;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.exceptions.SystemException;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.DropCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.RenameCollectionCommand;
import com.torodb.mongodb.commands.signatures.general.DeleteCommand;
import com.torodb.mongodb.commands.signatures.general.InsertCommand;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand;
import com.torodb.mongodb.repl.oplogreplier.fetcher.FilteredOplogFetcher;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import com.torodb.mongodb.utils.IndexPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.fi.util.function.CheckedFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ReplicationFilters {

  private static final Logger LOGGER = LogManager.getLogger(ReplicationFilters.class);

  private final ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> whitelist;
  private final ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> blacklist;
  private final DatabasePredicate databasePredicate = new DatabasePredicate();
  private final CollectionPredicate collectionPredicate = new CollectionPredicate();
  private final IndexPredicateImpl indexPredicate = new IndexPredicateImpl();
  private final OplogOperationPredicate oplogOperationPredicate = new OplogOperationPredicate();

  public ReplicationFilters(
      ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> whitelist,
      ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> blacklist) {
    super();
    this.whitelist = whitelist;
    this.blacklist = blacklist;
  }

  public Predicate<String> getDatabasePredicate() {
    return databasePredicate;
  }

  public BiPredicate<String, String> getCollectionPredicate() {
    return collectionPredicate;
  }

  public IndexPredicate getIndexPredicate() {
    return indexPredicate;
  }

  public Predicate<OplogOperation> getOperationPredicate() {
    return oplogOperationPredicate;
  }

  public OplogFetcher filterOplogFetcher(OplogFetcher originalFetcher) {
    return new FilteredOplogFetcher(oplogOperationPredicate, originalFetcher);
  }

  @FunctionalInterface
  public interface ResultFilter {

    public <R> Status<R> filter(Status<R> result);
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
  private boolean indexWhiteFilter(String database, String collection, String indexName,
      boolean unique, List<IndexOptions.Key> keys) {
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
              if (indexPattern.match(indexName, unique, keys)) {
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
  private boolean indexBlackFilter(String database, String collection, String indexName,
      boolean unique, List<IndexOptions.Key> keys) {
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

  private class DatabasePredicate implements Predicate<String> {

    @Override
    public boolean test(String database) {
      return databaseWhiteFilter(database) && databaseBlackFilter(database);
    }

  }

  private class CollectionPredicate implements BiPredicate<String, String> {

    @Override
    public boolean test(String database, String collection) {
      return collectionWhiteFilter(database, collection) && collectionBlackFilter(database,
          collection);
    }

  }

  public class IndexPredicateImpl implements IndexPredicate {

    @Override
    public boolean test(String database, String collection, String indexName, boolean unique,
        List<IndexOptions.Key> keys) {
      return indexWhiteFilter(database, collection, indexName, unique, keys) && indexBlackFilter(
          database, collection, indexName, unique, keys);
    }
  }

  @SuppressWarnings("checkstyle:LineLength")
  private static final ImmutableMap<Command<?, ?>, CheckedFunction<BsonDocument, String>> collectionCommands =
      ImmutableMap.<Command<?, ?>, CheckedFunction<BsonDocument, String>>builder()
          .put(CreateCollectionCommand.INSTANCE, d -> CreateCollectionCommand.INSTANCE
              .unmarshallArg(d).getCollection())
          .put(CreateIndexesCommand.INSTANCE, d -> CreateIndexesCommand.INSTANCE.unmarshallArg(d)
              .getCollection())
          .put(DropIndexesCommand.INSTANCE, d -> DropIndexesCommand.INSTANCE.unmarshallArg(d)
              .getCollection())
          .put(DropCollectionCommand.INSTANCE, d -> DropCollectionCommand.INSTANCE
              .unmarshallArg(d).getCollection())
          .put(RenameCollectionCommand.INSTANCE, d -> RenameCollectionCommand.INSTANCE
              .unmarshallArg(d).getFromCollection())
          .put(DeleteCommand.INSTANCE, d -> DeleteCommand.INSTANCE.unmarshallArg(d)
              .getCollection())
          .put(InsertCommand.INSTANCE, d -> InsertCommand.INSTANCE.unmarshallArg(d)
              .getCollection())
          .put(UpdateCommand.INSTANCE, d -> UpdateCommand.INSTANCE.unmarshallArg(d)
              .getCollection())
          .build();

  private class OplogOperationPredicate implements OplogOperationVisitor<Boolean, Void>,
      Predicate<OplogOperation> {

    @Override
    public Boolean visit(DbCmdOplogOperation op, Void arg) {
      if (op.getCommandName().isPresent()) {
        String commandName = op.getCommandName().get();
        if (collectionCommands.containsKey(commandName)) {
          try {
            assert op.getRequest() != null;

            String collection = collectionCommands.get(commandName)
                .apply(op.getRequest());
            return testCollection(commandName, op.getDatabase(), collection);
          } catch (Throwable e) {
            throw new SystemException("Error while parsing argument for command " + op
                .getCommandName(), e);
          }
        }
        return testDatabase(commandName, op.getDatabase());
      }
      return testDatabase("unknown", op.getDatabase());
    }

    @Override
    public Boolean visit(DbOplogOperation op, Void arg) {
      return testDatabase("unknown", op.getDatabase());
    }

    @Override
    public Boolean visit(DeleteOplogOperation op, Void arg) {
      return collectionPredicate.test(op.getDatabase(), op.getCollection());
    }

    @Override
    public Boolean visit(InsertOplogOperation op, Void arg) {
      return collectionPredicate.test(op.getDatabase(), op.getCollection());
    }

    @Override
    public Boolean visit(NoopOplogOperation op, Void arg) {
      return true;
    }

    @Override
    public Boolean visit(UpdateOplogOperation op, Void arg) {
      return collectionPredicate.test(op.getDatabase(), op.getCollection());
    }

    private boolean testDatabase(String commandName, String database) {
      if (databasePredicate.test(database)) {
        return true;
      }

      LOGGER.info("Skipping operation {} for filtered database {}.", commandName, database);

      return false;
    }

    private boolean testCollection(String commandName, String database, String collection) {
      if (collectionPredicate.test(database, collection)) {
        return true;
      }

      LOGGER.info("Skipping operation {} for filtered collection {}.{}.", commandName, database,
          collection);

      return false;
    }

    @Override
    public boolean test(OplogOperation t) {
      return t.accept(this, null);
    }

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
}
