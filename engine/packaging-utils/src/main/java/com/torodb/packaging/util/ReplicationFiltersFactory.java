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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.mongodb.repl.ReplicationFilters.IndexPattern;
import com.torodb.packaging.config.model.protocol.mongo.AbstractReplication;
import com.torodb.packaging.config.model.protocol.mongo.FilterList;
import com.torodb.packaging.config.model.protocol.mongo.FilterList.IndexFilter;
import com.torodb.packaging.config.util.SimpleRegExpDecoder;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public class ReplicationFiltersFactory {

  private static final Pattern ANY = SimpleRegExpDecoder.decode("*");
  private static final Splitter KEYS_SPLITTER = Splitter.on('.');

  private ReplicationFiltersFactory() {
  }

  public static ReplicationFilters getReplicationFilters(AbstractReplication replication) {
    ReplicationFilters replicationFilters = new ReplicationFilters(
        convertFilterList(replication.getInclude()),
        convertFilterList(replication.getExclude()));
    return replicationFilters;
  }

  @SuppressWarnings("checkstyle:LineLength")
  private static ImmutableMap<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> convertFilterList(
      FilterList filterList) {
    ImmutableMap.Builder<Pattern, ImmutableMap<Pattern, ImmutableList<IndexPattern>>> filterBuilder 
        = ImmutableMap.builder();

    if (filterList != null) {
      for (Map.Entry<String, Map<String, List<IndexFilter>>> databaseEntry : filterList.entrySet()) {
        ImmutableMap.Builder<Pattern, ImmutableList<IndexPattern>> collectionsBuilder = ImmutableMap
            .builder();
        for (Map.Entry<String, List<IndexFilter>> collection : databaseEntry.getValue().entrySet()) {
          ImmutableList.Builder<IndexPattern> indexesBuilder = ImmutableList.builder();
          for (IndexFilter indexFilter : collection.getValue()) {
            Pattern indexNamePattern = ANY;
            if (indexFilter.getName() != null) {
              indexNamePattern = SimpleRegExpDecoder.decode(indexFilter.getName());
            }
            IndexPattern.Builder indexPatternBuilder = new IndexPattern.Builder(indexNamePattern,
                indexFilter.getUnique());
            for (Map.Entry<String, String> indexFieldFilter : indexFilter.getKeys().entrySet()) {
              ImmutableList.Builder<Pattern> fieldReferencePatternBuilder = ImmutableList.builder();
              for (String indexFieldKeyFilter : KEYS_SPLITTER.split(indexFieldFilter.getKey())) {
                fieldReferencePatternBuilder.add(SimpleRegExpDecoder.decode(indexFieldKeyFilter));
              }
              indexPatternBuilder.addFieldPattern(fieldReferencePatternBuilder.build(),
                  SimpleRegExpDecoder.decode(FilterList.getIndexType(indexFieldFilter.getValue())
                      .getName()));
            }
            indexesBuilder.add(indexPatternBuilder.build());
          }
          collectionsBuilder.put(SimpleRegExpDecoder.decode(collection.getKey()), indexesBuilder
              .build());
        }
        filterBuilder.put(SimpleRegExpDecoder.decode(databaseEntry.getKey()), collectionsBuilder
            .build());
      }
    }

    return filterBuilder.build();
  }
}
