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
package com.torodb.packaging.util;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.mongodb.repl.ReplicationFilters.IndexPattern;
import com.torodb.packaging.config.model.protocol.mongo.FilterList;
import com.torodb.packaging.config.model.protocol.mongo.FilterList.IndexFilter;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.config.util.SimpleRegExpDecoder;

/**
 *
 */
public class ReplicationFiltersFactory {
    
    private final static Pattern ANY = SimpleRegExpDecoder.decode("*");
    private final static Splitter KEYS_SPLITTER = Splitter.on('.');

    private ReplicationFiltersFactory() {}

    public static ReplicationFilters getReplicationFilters(Replication replication) {
        ReplicationFilters replicationFilters = new ReplicationFilters(
                convertFilterList(replication.getInclude()), 
                convertFilterList(replication.getExclude()));
        return replicationFilters;
    }

    private static ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> convertFilterList(
            FilterList filterList) {
        ImmutableMap.Builder<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterBuilder = ImmutableMap.builder();
        
        if (filterList != null) {
            for (Map.Entry<String, Map<String, List<IndexFilter>>> databaseEntry : filterList.entrySet()) {
                ImmutableMap.Builder<Pattern, ImmutableList<IndexPattern>> collectionsBuilder = ImmutableMap.builder();
                for (Map.Entry<String, List<IndexFilter>> collection : databaseEntry.getValue().entrySet()) {
                    ImmutableList.Builder<IndexPattern> indexesBuilder = ImmutableList.builder();
                    for (IndexFilter indexFilter : collection.getValue()) {
                        Pattern indexNamePattern = ANY;
                        if (indexFilter.getName() != null) {
                            indexNamePattern = SimpleRegExpDecoder.decode(indexFilter.getName());
                        }
                        IndexPattern.Builder indexPatternBuilder = new IndexPattern.Builder(indexNamePattern, indexFilter.getUnique());
                        for (Map.Entry<String, String> indexFieldFilter : indexFilter.getKeys().entrySet()) {
                            ImmutableList.Builder<Pattern> fieldReferencePatternBuilder = ImmutableList.builder();
                            for (String indexFieldKeyFilter : KEYS_SPLITTER.split(indexFieldFilter.getKey())) {
                                fieldReferencePatternBuilder.add(SimpleRegExpDecoder.decode(indexFieldKeyFilter));
                            }
                            indexPatternBuilder.addFieldPattern(fieldReferencePatternBuilder.build(), 
                                    SimpleRegExpDecoder.decode(FilterList.getIndexType(indexFieldFilter.getValue()).name()));
                        }
                        indexesBuilder.add(indexPatternBuilder.build());
                    }
                    collectionsBuilder.put(SimpleRegExpDecoder.decode(collection.getKey()), indexesBuilder.build());
                }
                filterBuilder.put(SimpleRegExpDecoder.decode(databaseEntry.getKey()), collectionsBuilder.build());
            }
        }
        
        return filterBuilder.build();
    }
}
