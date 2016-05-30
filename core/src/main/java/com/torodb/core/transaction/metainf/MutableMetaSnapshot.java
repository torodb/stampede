
package com.torodb.core.transaction.metainf;

import com.torodb.core.annotations.DoNotChange;

/**
 *
 * @param <MMD>
 */
public interface MutableMetaSnapshot<MMD extends MutableMetaDatabase> extends MetaSnapshot<MMD> {

    public abstract MMD addMetaDatabase(String dbName, String dbId);

    @DoNotChange
    public abstract Iterable<MMD> getModifiedDatabases();

    public abstract ImmutableMetaSnapshot immutableCopy();
}
