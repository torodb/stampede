package com.torodb.backend.interfaces;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.Configuration;
import org.jooq.Field;

public interface StructureInterface {
    @Nonnull String createSchemaStatement(@Nonnull String schemaName);
    @Nonnull String dropSchemaStatement(@Nonnull String schemaName);
    @Nonnull String createIndexStatement(@Nonnull Configuration conf, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String fieldName);
    @Nonnull String createDocPartTableStatement(@Nonnull Configuration conf, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull Collection<Field<?>> fields);
    @Nonnull String addColumnsToDocPartTableStatement(@Nonnull Configuration conf, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull Collection<Field<?>> fields);
}
