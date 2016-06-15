package com.torodb.backend.interfaces;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.d2r.DocPartData;

public interface WriteInterface {
    void insertDocPartData(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull DocPartData docPartData);

    @Nonnull String deleteDidsStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull String didColumnName);
    void setDeleteDidsStatementParameters(@Nonnull PreparedStatement ps, @Nonnull Collection<Integer> dids) throws SQLException;
}
