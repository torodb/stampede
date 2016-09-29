package com.torodb.mongodb.utils;

import java.util.List;
import java.util.Map;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions.IndexType;

public interface IndexPredicate {
    public boolean test(String database, String collection, String indexName, boolean unique, Map<List<String>, IndexType> keys);
}
