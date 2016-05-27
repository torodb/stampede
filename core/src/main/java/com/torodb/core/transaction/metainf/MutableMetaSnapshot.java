
package com.torodb.core.transaction.metainf;

/**
 *
 * @param <MMD>
 */
public interface MutableMetaSnapshot<MMD extends MutableMetaDatabase> extends MetaSnapshot<MMD> {

    public abstract void addMetaDatabase(MMD database);

    public abstract MMD addMetaDatabase(String dbName, String dbId);
    
}
