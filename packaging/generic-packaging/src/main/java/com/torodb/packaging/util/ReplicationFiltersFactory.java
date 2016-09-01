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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.packaging.config.model.protocol.mongo.FilterList;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.config.util.SimpleRegExpDecoder;

public class ReplicationFiltersFactory {

    public static ReplicationFilters getReplicationFilters(Replication replication) {
        ReplicationFilters replicationFilters = new ReplicationFilters(
                convertFilterList(replication.getInclude()), 
                convertFilterList(replication.getExclude()));
        return replicationFilters;
    }

    private static ImmutableMap<Pattern, ImmutableList<Pattern>> convertFilterList(
            FilterList filterList) {
        ImmutableMap.Builder<Pattern, ImmutableList<Pattern>> filterBuilder = ImmutableMap.builder();
        
        if (filterList != null) {
            for (Map.Entry<String, List<String>> databaseEntry : filterList.entrySet()) {
                ImmutableList.Builder<Pattern> collectionsBuilder = ImmutableList.builder();
                for (String collection : databaseEntry.getValue()) {
                    collectionsBuilder.add(SimpleRegExpDecoder.decode(collection));
                }
                filterBuilder.put(SimpleRegExpDecoder.decode(databaseEntry.getKey()), collectionsBuilder.build());
            }
        }
        
        return filterBuilder.build();
    }

}
