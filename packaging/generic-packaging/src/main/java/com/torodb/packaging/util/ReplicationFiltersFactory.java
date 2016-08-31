
package com.torodb.packaging.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.protocol.mongo.FilterList;
import com.torodb.packaging.config.util.SimpleRegExpDecoder;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public class ReplicationFiltersFactory {

    private ReplicationFiltersFactory() {}

    public static ReplicationFilters fromConfig(Config config) {
        ReplicationFilters filterProvider = new ReplicationFilters(
                convertFilterList(config.getProtocol().getMongo().getReplication().get(0).getInclude()),
                convertFilterList(config.getProtocol().getMongo().getReplication().get(0).getExclude()));
        return filterProvider;
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
