
package com.torodb.torod;

import javax.annotation.Nonnull;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.google.common.collect.ImmutableList;
import com.torodb.core.language.AttributeReference;

/**
 *
 */
public class IndexInfo {
    
    private final String name;
    private final boolean unique;
    private final JsonObject properties;
    private final ImmutableList<IndexFieldInfo> fields;

    public IndexInfo(@Nonnull String name, boolean unique, @Nonnull JsonObject properties,
            ImmutableList<IndexFieldInfo> fields) {
        this.name = name;
        this.unique = unique;
        this.properties = properties;
        this.fields = fields;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public boolean isUnique() {
        return unique;
    }

    @Nonnull
    public JsonObject getProperties() {
        return properties;
    }

    @Nonnull
    public ImmutableList<IndexFieldInfo> getFields() {
        return fields;
    }

    public static class Builder {
        private final String name;
        private final boolean isUnique;
        private final JsonObjectBuilder propertiesBuilder;
        private final ImmutableList.Builder<IndexFieldInfo> fieldsBuilder;
        
        public Builder(@Nonnull String name, boolean isUnique) {
            this.name = name;
            this.isUnique = isUnique;
            this.propertiesBuilder = Json.createObjectBuilder();
            this.fieldsBuilder = ImmutableList.builder();
        }
        
        public Builder addField(AttributeReference attributeReference, boolean isAscending) {
            fieldsBuilder.add(new IndexFieldInfo(attributeReference, isAscending));
            return this;
        }
        
        public IndexInfo build() {
            return new IndexInfo(name, isUnique, 
                    propertiesBuilder.build(), 
                    fieldsBuilder.build());
        }
    }
    
}
