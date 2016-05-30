package com.torodb.poc.backend.interfaces;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.poc.backend.TableToDDL.TableData;
import com.torodb.poc.backend.TableToDDL.PathSnapshot;
import com.torodb.poc.backend.mocks.ImplementationDbException;
import com.torodb.poc.backend.mocks.RetryTransactionException;

public interface WriteInterface {
    void insertPathDocuments(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull PathSnapshot pathSnapshot, @Nonnull TableData tableData) throws ImplementationDbException, RetryTransactionException;

    @Nonnull String deleteDidsStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull String didColumnName);
    void setDeleteDidsStatementParameters(@Nonnull PreparedStatement ps, @Nonnull Collection<Integer> dids) throws SQLException;
}
