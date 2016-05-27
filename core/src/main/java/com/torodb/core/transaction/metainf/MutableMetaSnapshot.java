
package com.torodb.core.transaction.metainf;

import java.util.Collections;

/**
 *
 * @param <MMD>
 */
public interface MutableMetaSnapshot<MMD extends MutableMetaDatabase> extends MetaSnapshot<MetaDatabase> {

    public default MMD addDatabase(String dbName, String dbId) {
        return addDatabase(dbName, dbId, Collections.emptyList());
    }

    public abstract MMD addDatabase(String dbName, String dbId, Iterable<ImmutableMetaField> fields);

    /**
     *
     * @param dbName
     * @return
     * @throws IllegalArgumentException if {@link #getDatabaseByName(java.lang.String)}
     *                                  returns null using the same name
     */
    public abstract MMD asMutableDatabase(String dbName) throws IllegalArgumentException;
    
}
