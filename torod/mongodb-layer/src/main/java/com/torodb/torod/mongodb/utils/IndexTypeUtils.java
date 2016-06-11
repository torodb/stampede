
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.pojos.IndexedAttributes.IndexType;

/**
 *
 */
public class IndexTypeUtils {

    public static IndexType toIndexType(IndexOptions.IndexType indexType) {
        switch(indexType) {
            case asc:
                return IndexType.asc;
            case desc:
                return IndexType.desc;
            case text:
                return IndexType.text;
            case geospatial:
                return IndexType.geospatial;
            case hashed:
                return IndexType.hashed;
        }
        
        throw new ToroRuntimeException("Unknown index of type " + indexType);
    }

    public static IndexOptions.IndexType fromIndexType(IndexType indexType) {
        switch(indexType) {
            case asc:
                return IndexOptions.IndexType.asc;
            case desc:
                return IndexOptions.IndexType.desc;
            case text:
                return IndexOptions.IndexType.text;
            case geospatial:
                return IndexOptions.IndexType.geospatial;
            case hashed:
                return IndexOptions.IndexType.hashed;
        }
        
        throw new ToroRuntimeException("Unknown index of type " + indexType);
    }
}
