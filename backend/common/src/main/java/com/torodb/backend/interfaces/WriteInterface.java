package com.torodb.backend.interfaces;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.RollbackException;

public interface WriteInterface {
    void insertPathDocuments(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull DocPartData docPartData) throws RollbackException;

    @Nonnull String deleteDidsStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull String didColumnName);
    void setDeleteDidsStatementParameters(@Nonnull PreparedStatement ps, @Nonnull Collection<Integer> dids) throws SQLException;
}
