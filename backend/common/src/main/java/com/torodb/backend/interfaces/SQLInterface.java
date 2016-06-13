package com.torodb.backend.interfaces;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

public interface SQLInterface {
    public boolean isRestrictedSchemaName(@Nonnull String schemaName);
    public boolean isRestrictedColumnName(@Nonnull String columnName);
    @Nonnull String arrayUnnestParametrizedSelectStatement();
    @Nonnull ResultSet getColumns(@Nonnull DatabaseMetaData metadata, @Nonnull String schemaName, @Nonnull String tableName) throws SQLException;
    @Nonnull ResultSet getIndexes(@Nonnull DatabaseMetaData metadata, @Nonnull String schemaName, @Nonnull String tableName) throws SQLException;
}
