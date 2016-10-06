package com.torodb.mongodb.utils;

import java.util.List;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;

public interface IndexPredicate {
    public boolean test(String database, String collection, String indexName, boolean unique, List<IndexOptions.Key> keys);
}
