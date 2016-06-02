package com.torodb.backend.interfaces;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

public interface SQLInterface {
    @Nonnull String arrayUnnestParametrizedSelectStatement();
    @Nonnull ResultSet getColumns(@Nonnull DatabaseMetaData metadata, @Nonnull String schemaName, @Nonnull String tableName) throws SQLException;
    @Nonnull ResultSet getIndexes(@Nonnull DatabaseMetaData metadata, @Nonnull String schemaName, @Nonnull String tableName) throws SQLException;
}
