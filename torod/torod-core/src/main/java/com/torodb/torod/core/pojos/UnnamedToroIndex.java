
package com.torodb.torod.core.pojos;

import com.torodb.torod.core.language.AttributeReference;

/**
 *
 */
public final class UnnamedToroIndex implements ToroIndex {

    private final String database;
    private final String collection;
    private final boolean unique;
    private final IndexedAttributes attributes;

    public UnnamedToroIndex(
            String database, 
            String collection, 
            boolean unique, 
            IndexedAttributes attributes) {
        this.database = database;
        this.collection = collection;
        this.unique = unique;
        this.attributes = attributes;
    }
    
    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getCollection() {
        return collection;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public IndexedAttributes getAttributes() {
        return attributes;
    }

    @Override
    public UnnamedToroIndex asUnnamed() {
        return this;
    }

    public static class Builder {
        private String name;
        private final IndexedAttributes.Builder attributesBuilder 
                = new IndexedAttributes.Builder();
        private String database;
        private String collection;
        private boolean unique;

        public String getName() {
            return name;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public String getDatabaseName() {
            return database;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public String getCollection() {
            return collection;
        }

        public Builder setCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public boolean isUnique() {
            return unique;
        }

        public Builder setUnique(boolean unique) {
            this.unique = unique;
            return this;
        }
        
        public Builder addIndexedAttribute(
                AttributeReference attRef, 
                boolean ascendingOrder) {
            this.attributesBuilder.addAttribute(attRef, ascendingOrder);
            return this;
        }
        
        public UnnamedToroIndex build() {
            return new UnnamedToroIndex(
                    database, 
                    collection, 
                    unique,
                    attributesBuilder.build()
            );
        }
        
    }
    
}
