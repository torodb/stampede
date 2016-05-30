package com.torodb.poc.backend.interfaces;

import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.Configuration;
import org.jooq.Field;

import com.torodb.poc.backend.tables.PathDocTable;

public interface StructureInterface {
    @Nonnull String createSchemaStatement(@Nonnull String schemaName);
    @Nonnull String dropSchemaStatement(@Nonnull String schemaName);
    @Nonnull String createIndexStatement(PathDocTable table, Field<?> field, Configuration conf);
    @Nonnull String createPathDocTableStatement(String schemaName, String tableName, List<Field<?>> fields, Configuration conf);
    @Nonnull String addColumnsToTableStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull List<Field<?>> fields, @Nonnull Configuration conf);
}
