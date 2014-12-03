
package com.torodb.torod.core.pojos;

/**
 *
 */
public class DefaultNamedToroIndex implements NamedToroIndex {

    private final String name;
    private final UnnamedToroIndex unnamedIndex;

    public DefaultNamedToroIndex(
            String name, 
            IndexedAttributes attributes, 
            String databaseName, 
            String collection, 
            boolean unique) {
        this.name = name;
        this.unnamedIndex = new UnnamedToroIndex(databaseName, collection, unique, attributes);
    }

    public DefaultNamedToroIndex(String name, UnnamedToroIndex unnamedIndex) {
        this.name = name;
        this.unnamedIndex = unnamedIndex;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public UnnamedToroIndex asUnnamed() {
        return unnamedIndex;
    }

    @Override
    public String getDatabase() {
        return unnamedIndex.getDatabase();
    }

    @Override
    public String getCollection() {
        return unnamedIndex.getCollection();
    }

    @Override
    public boolean isUnique() {
        return unnamedIndex.isUnique();
    }

    @Override
    public IndexedAttributes getAttributes() {
        return unnamedIndex.getAttributes();
    }
}
