
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;

/**
 *
 */
public class DefaultIdUtils {

    public static final String DEFAULT_ID_KEY = "$_id";

    private DefaultIdUtils() {}

    public static boolean containsDefaultId(BsonDocument doc) {
        return doc.containsKey(DEFAULT_ID_KEY);
    }

    public static BsonValue getDefaultId(BsonDocument doc) {
        return doc.get(DEFAULT_ID_KEY);
    }
}
