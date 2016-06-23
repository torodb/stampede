package com.torodb.backend;

import java.util.Collection;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;
import org.jooq.Field;

public interface StructureInterface {
    void createSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName);
    void dropSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName);
    void createDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull Collection<? extends Field<?>> fields);
    void addColumnToDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull Field<?> field);
    
    void createIndex(@Nonnull DSLContext dsl, @Nonnull String fullIndexName, @Nonnull String tableSchema, 
            @Nonnull String tableName, @Nonnull String tableColumnName, boolean isAscending);
    void dropIndex(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String indexName);
}
