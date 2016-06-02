
package com.torodb.backend.converters.json;

import org.jooq.Converter;

import com.torodb.backend.mocks.NamedToroIndex;
import com.torodb.backend.mocks.ToroImplementationException;

/**
 *
 */
public class ToroIndexToJsonConverter implements Converter<String, NamedToroIndex> {
    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String collectionName;
    
    private static final String ATTS_KEY = "atts";
    private static final String UNIQUE_KEY = "unique";
    private static final String NAME_KEY = "key";
    private static final String DESCENDING = "desc";

    public ToroIndexToJsonConverter(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    @Override
    public NamedToroIndex from(String databaseObject) {
        throw new ToroImplementationException("Not implemented yet");
    }

    @Override
    public String to(NamedToroIndex userObject) {
        throw new ToroImplementationException("Not implemented yet");
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<NamedToroIndex> toType() {
        return NamedToroIndex.class;
    }
    
}
