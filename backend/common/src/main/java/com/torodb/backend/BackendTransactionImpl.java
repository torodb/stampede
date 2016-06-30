
package com.torodb.backend;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.backend.BackendTransaction;
import com.torodb.core.backend.DidCursor;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jooq.DSLContext;

/**
 *
 */
public abstract class BackendTransactionImpl implements BackendTransaction {

    private boolean closed = false;
    private final Connection connection;
    private final DSLContext dsl;
    private final SqlInterface sqlInterface;
    private final BackendConnectionImpl backendConnection;
    private final R2DTranslator<ResultSet> r2dTranslator;

    public BackendTransactionImpl(Connection connection, SqlInterface sqlInterface,
            BackendConnectionImpl backendConnection, R2DTranslator<ResultSet> r2dTranslator) {
        this.connection = connection;
        this.dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
        this.sqlInterface = sqlInterface;
        this.backendConnection = backendConnection;
        this.r2dTranslator = r2dTranslator;
    }

    boolean isClosed() {
        return closed;
    }

    Connection getConnection() {
        return connection;
    }

    DSLContext getDsl() {
        return dsl;
    }

    SqlInterface getSqlInterface() {
        return sqlInterface;
    }

    BackendConnectionImpl getBackendConnection() {
        return backendConnection;
    }

    @Override
    public Cursor<ToroDocument> findAll(MetaDatabase db, MetaCollection col) {
        try {
            DidCursor allDids = sqlInterface.getReadInterface().getAllCollectionDids(dsl, db, col);
            return new DefaultCursor(sqlInterface, r2dTranslator, allDids, dsl, db, col);
        } catch (SQLException ex) {
            sqlInterface.getErrorHandler().handleRollbackException(Context.fetch, ex);
            throw new AssertionError();
        }
    }

    @Override
    public Cursor<ToroDocument> findByField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField field, KVValue<?> value) {
        try {
            DidCursor allDids = sqlInterface.getReadInterface().getCollectionDidsWithFieldEqualsTo(dsl, db, col, docPart, field, value);
            return new DefaultCursor(sqlInterface, r2dTranslator, allDids, dsl, db, col);
        } catch (SQLException ex) {
            sqlInterface.getErrorHandler().handleRollbackException(Context.fetch, ex);
            throw new AssertionError();
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            backendConnection.onTransactionClosed(this);
            try {
                connection.rollback();
                connection.close();
            } catch (SQLException ex) {
                sqlInterface.getErrorHandler().handleRollbackException(Context.close, ex);
            }
            dsl.close();
        }
    }


}
