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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.lambda.fi.util.function.CheckedFunction;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropCollectionCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.RenameCollectionCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions.IndexType;
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
import com.torodb.mongodb.language.utils.NamespaceUtil;
import com.torodb.mongodb.repl.oplogreplier.fetcher.FilteredOplogFetcher;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import com.torodb.mongodb.utils.IndexPredicate;

public class ReplicationFilters {

    private final ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> whitelist;
    private final ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> blacklist;
    private final DatabasePredicate databasePredicate = new DatabasePredicate();
    private final CollectionPredicate collectionPredicate = new CollectionPredicate();
    private final IndexPredicateImpl indexPredicate = new IndexPredicateImpl();
    private final OplogOperationPredicate oplogOperationPredicate = new OplogOperationPredicate(); 

    public ReplicationFilters(ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> whitelist,
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
    
    private boolean databaseWhiteFilter(String database) {
        if (whitelist.isEmpty()) {
            return true;
        }
        
        for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : whitelist.entrySet()) {
            Matcher databaseMatcher = filterEntry.getKey().matcher(database);
            if (databaseMatcher.matches()) {
                return true;
            }
        }
        
        return false;
    }
    
    private boolean databaseBlackFilter(String database) {
        if (blacklist.isEmpty()) {
            return true;
        }

        for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : blacklist.entrySet()) {
            Matcher databaseMatcher = filterEntry.getKey().matcher(database);
            if (databaseMatcher.matches()) {
                if (filterEntry.getValue().isEmpty()) {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    private boolean collectionWhiteFilter(String database, String collection) {
        if (whitelist.isEmpty()) {
            return true;
        }
        
        for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : whitelist.entrySet()) {
            Matcher databaseMatcher = filterEntry.getKey().matcher(database);
            if (databaseMatcher.matches()) {
                if (filterEntry.getValue().isEmpty()) {
                    return true;
                }
                
                for (Map.Entry<Pattern, ImmutableList<IndexPattern>> collectionPattern : filterEntry.getValue().entrySet()) {
                    Matcher collectionMatcher = collectionPattern.getKey().matcher(collection);
                    if (collectionMatcher.matches()) {
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
    
    private boolean collectionBlackFilter(String database, String collection) {
        if (blacklist.isEmpty()) {
            return true;
        }

        for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : blacklist.entrySet()) {
            Matcher databaseMatcher = filterEntry.getKey().matcher(database);
            if (databaseMatcher.matches()) {
                if (filterEntry.getValue().isEmpty()) {
                    return false;
                }
                
                for (Map.Entry<Pattern, ImmutableList<IndexPattern>> collectionPattern : filterEntry.getValue().entrySet()) {
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
    
    private boolean indexWhiteFilter(String database, String collection, String indexName, boolean unique, Map<List<String>, IndexType> keys) {
        if (whitelist.isEmpty()) {
            return true;
        }
        
        for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : whitelist.entrySet()) {
            Matcher databaseMatcher = filterEntry.getKey().matcher(database);
            if (databaseMatcher.matches()) {
                if (filterEntry.getValue().isEmpty()) {
                    return true;
                }
                
                for (Map.Entry<Pattern, ImmutableList<IndexPattern>> collectionPattern : filterEntry.getValue().entrySet()) {
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
    
    private boolean indexBlackFilter(String database, String collection, String indexName, boolean unique, Map<List<String>, IndexType> keys) {
        if (blacklist.isEmpty()) {
            return true;
        }

        for (Map.Entry<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterEntry : blacklist.entrySet()) {
            Matcher databaseMatcher = filterEntry.getKey().matcher(database);
            if (databaseMatcher.matches()) {
                if (filterEntry.getValue().isEmpty()) {
                    return false;
                }
                
                for (Map.Entry<Pattern, ImmutableList<IndexPattern>> collectionPattern : filterEntry.getValue().entrySet()) {
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
            return databaseWhiteFilter(database) &&
                    databaseBlackFilter(database);
        }
        
    }
    
    private class CollectionPredicate implements BiPredicate<String, String> {
        @Override
        public boolean test(String database, String collection) {
            return collectionWhiteFilter(database, collection) &&
                    collectionBlackFilter(database, collection);
        }
        
    }
    
    public class IndexPredicateImpl implements IndexPredicate {
        @Override
        public boolean test(String database, String collection, String indexName, boolean unique, Map<List<String>, IndexType> keys) {
            return indexWhiteFilter(database, collection, indexName, unique, keys) &&
                    indexBlackFilter(database, collection, indexName, unique, keys);
        }
    }
    
    private static final ImmutableMap<Command<?, ?>, CheckedFunction<BsonDocument, String>> collectionCommands = 
        ImmutableMap.<Command<?, ?>, CheckedFunction<BsonDocument, String>>builder()
            .put(CreateCollectionCommand.INSTANCE, d -> CreateCollectionCommand.INSTANCE.unmarshallArg(d).getCollection())
            .put(DropIndexesCommand.INSTANCE, d -> DropIndexesCommand.INSTANCE.unmarshallArg(d).getCollection())
            .put(DropCollectionCommand.INSTANCE, d -> DropCollectionCommand.INSTANCE.unmarshallArg(d).getCollection())
            .put(RenameCollectionCommand.INSTANCE, d -> RenameCollectionCommand.INSTANCE.unmarshallArg(d).getFromCollection())
            .put(DeleteCommand.INSTANCE, d -> DeleteCommand.INSTANCE.unmarshallArg(d).getCollection())
            .put(InsertCommand.INSTANCE, d -> InsertCommand.INSTANCE.unmarshallArg(d).getCollection())
            .put(UpdateCommand.INSTANCE, d -> UpdateCommand.INSTANCE.unmarshallArg(d).getCollection())
            .build();
    
    private static final ImmutableMap<Command<?, ?>, CheckedFunction<BsonDocument, BiFunction<String, IndexPredicateImpl, Boolean>>> indexCommands = 
        ImmutableMap.<Command<?, ?>, CheckedFunction<BsonDocument, BiFunction<String, IndexPredicateImpl, Boolean>>>builder()
            .put(CreateIndexesCommand.INSTANCE, d -> {
                CreateIndexesArgument arg = CreateIndexesCommand.INSTANCE.unmarshallArg(d);
                
                return (database, indexPredicate) -> {
                    for (IndexOptions indexOptions : arg.getIndexesToCreate()) {
                        if (!indexPredicate.test(database, arg.getCollection(), 
                                indexOptions.getName(), 
                                indexOptions.isUnique(), 
                                indexOptions.getKeys().entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())))) {
                            return false;
                        }
                    }
                    return true;
                };
            })
            .build();
    
    private class OplogOperationPredicate implements OplogOperationVisitor<Boolean, Void>, Predicate<OplogOperation> {

        @Override
        public Boolean visit(DbCmdOplogOperation op, Void arg) {
            if (op.getCommandName().isPresent()) {
                if (collectionCommands.containsKey(op.getCommandName().get())) {
                    try {
                        assert op.getRequest() != null;
                        
                        String collection = collectionCommands.get(op.getCommandName().get())
                                .apply(op.getRequest());
                        return collectionPredicate.test(op.getDatabase(), collection);
                    } catch (Throwable e) {
                        throw new SystemException("Error while parsing argument for command " + op.getCommandName(), e);
                    }
                }
                if (indexCommands.containsKey(op.getCommandName().get())) {
                    try {
                        assert op.getRequest() != null;
                        
                        return indexCommands.get(op.getCommandName().get())
                                .apply(op.getRequest())
                                .apply(op.getDatabase(), indexPredicate);
                    } catch (Throwable e) {
                        throw new SystemException("Error while parsing argument for command " + op.getCommandName(), e);
                    }
                }
            }
            return databasePredicate.test(op.getDatabase());
        }

        @Override
        public Boolean visit(DbOplogOperation op, Void arg) {
            return databasePredicate.test(op.getDatabase());
        }

        @Override
        public Boolean visit(DeleteOplogOperation op, Void arg) {
            return collectionPredicate.test(op.getDatabase(), op.getCollection());
        }

        @Override
        public Boolean visit(InsertOplogOperation op, Void arg) {
            if (NamespaceUtil.INDEXES_COLLECTION.equals(op.getCollection())) {
                try {
                    IndexOptions indexOptions = IndexOptions.unmarshall(op.getDocToInsert());
                    if (!indexPredicate.test(op.getDatabase(), op.getCollection(), 
                            indexOptions.getName(), 
                            indexOptions.isUnique(), 
                            indexOptions.getKeys().entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())))) {
                        return false;
                    }
                    
                    return true;
                } catch (Throwable e) {
                    throw new SystemException("Error while parsing argument for insert on collection " + NamespaceUtil.INDEXES_COLLECTION, e);
                }
            }
            
            return collectionPredicate.test(op.getDatabase(), op.getCollection());
        }

        @Override
        public Boolean visit(NoopOplogOperation op, Void arg) {
            return databasePredicate.test(op.getDatabase());
        }

        @Override
        public Boolean visit(UpdateOplogOperation op, Void arg) {
            return collectionPredicate.test(op.getDatabase(), op.getCollection());
        }

        @Override
        public boolean test(OplogOperation t) {
            return t.accept(this, null);
        }
        
    }
    
    public static class IndexPattern {
        private final Pattern name;
        private final Boolean unique;
        private final ImmutableMap<ImmutableList<Pattern>, Pattern> fieldsPattern;
        
        public IndexPattern(@Nonnull Pattern name, @Nullable Boolean unique, @Nonnull ImmutableMap<ImmutableList<Pattern>, Pattern> fieldsPattern) {
            super();
            this.name = name;
            this.unique = unique;
            this.fieldsPattern = fieldsPattern;
        }
        
        public boolean match(String name, boolean unique, Map<List<String>, IndexType> fields) {
            if (this.name.matcher(name).matches() && 
                    (this.unique == null || this.unique.booleanValue() == unique) &&
                    (this.fieldsPattern.isEmpty() || this.fieldsPattern.size() == fields.size())) {
                if (this.fieldsPattern.isEmpty()) {
                    return true;
                }
                
                Iterator<Map.Entry<List<String>, IndexType>> fieldIterator = fields.entrySet().iterator();
                Iterator<Map.Entry<ImmutableList<Pattern>, Pattern>> fieldPatternIterator = fieldsPattern.entrySet().iterator();
                while (fieldPatternIterator.hasNext() &&
                        fieldIterator.hasNext()) {
                    Map.Entry<ImmutableList<Pattern>, Pattern> fieldPattern = fieldPatternIterator.next();
                    Map.Entry<List<String>, IndexType> field = fieldIterator.next();
                    if (!fieldPattern.getValue().matcher(field.getValue().name()).matches() ||
                            fieldPattern.getKey().size() != field.getKey().size()) {
                        return false;
                    }
                    Iterator<Pattern> fieldReferencePatternIterator = fieldPattern.getKey().iterator();
                    Iterator<String> fieldReferenceIterator = field.getKey().iterator();
                    while (fieldReferencePatternIterator.hasNext() && 
                            fieldReferenceIterator.hasNext()) {
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
            private final Map<ImmutableList<Pattern>, Pattern> fieldsPattern =
                    new LinkedHashMap<>();
            
            public Builder(@Nonnull Pattern name, @Nullable Boolean unique) {
                this.name = name;
                this.unique = unique;
            }
            
            public Builder addFieldPattern(ImmutableList<Pattern> fieldReferencePattern, Pattern typePattern) {
                fieldsPattern.put(fieldReferencePattern, typePattern);
                return this;
            }
            
            public IndexPattern build() {
                return new IndexPattern(name, unique, ImmutableMap.copyOf(fieldsPattern));
            }
        }
    }
}
