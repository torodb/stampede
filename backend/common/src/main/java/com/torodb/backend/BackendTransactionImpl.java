
package com.torodb.backend;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import org.jooq.DSLContext;

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

/**
 *
 */
public abstract class BackendTransactionImpl implements BackendTransaction {

    private boolean closed = false;
    private final Connection connection;
    private final DSLContext dsl;
    private final SqlInterface sqlInterface;
    private final BackendConnectionImpl backendConnection;
    private final R2DTranslator r2dTranslator;

    public BackendTransactionImpl(Connection connection, SqlInterface sqlInterface,
            BackendConnectionImpl backendConnection, R2DTranslator r2dTranslator) {
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

    R2DTranslator getR2dTranslator() {
        return r2dTranslator;
    }

    public long getDatabaseSize(MetaDatabase db) {
        return sqlInterface.getMetaDataReadInterface().getDatabaseSize(getDsl(), db);
    }

    public long countAll(MetaDatabase db, MetaCollection col) {
        return sqlInterface.getReadInterface().countAll(getDsl(), db, col);
    }

    public long getCollectionSize(MetaDatabase db, MetaCollection col) {
        return sqlInterface.getMetaDataReadInterface().getCollectionSize(getDsl(), db, col);
    }

    public long getDocumentsSize(MetaDatabase db, MetaCollection col) {
        return sqlInterface.getMetaDataReadInterface().getDocumentsSize(getDsl(), db, col);
    }

    @Override
    public Cursor<ToroDocument> findAll(MetaDatabase db, MetaCollection col) {
        try {
            DidCursor allDids = sqlInterface.getReadInterface().getAllCollectionDids(dsl, db, col);
            return new DefaultCursor(sqlInterface, r2dTranslator, allDids, dsl, db, col);
        } catch (SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
        }
    }

    @Override
    public Cursor<ToroDocument> findByField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField field, KVValue<?> value) {
        try {
            DidCursor allDids = sqlInterface.getReadInterface().getCollectionDidsWithFieldEqualsTo(dsl, db, col, docPart, field, value);
            return new DefaultCursor(sqlInterface, r2dTranslator, allDids, dsl, db, col);
        } catch (SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
        }
    }

    @Override
    public DidCursor findAllDids(MetaDatabase db, MetaCollection col) {
        try {
            return sqlInterface.getReadInterface().getAllCollectionDids(dsl, db, col);
        } catch (SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
        }
    }

    @Override
    public DidCursor findDidsByField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField field, KVValue<?> value) {
        try {
            return sqlInterface.getReadInterface().getCollectionDidsWithFieldEqualsTo(dsl, db, col, docPart, field, value);
        } catch (SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
        }
    }

    @Override
    public Collection<ToroDocument> readDocuments(MetaDatabase db, MetaCollection col, Collection<Integer> dids) {
        try {
            DocPartResultBatch docPartResultBatch = sqlInterface.getReadInterface().getCollectionResultSets(getDsl(), db, col, dids);
            return r2dTranslator.translate(docPartResultBatch);
        } catch (SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
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
                sqlInterface.getErrorHandler().handleException(Context.CLOSE, ex);
            } finally {
                dsl.close();
            }
        }
    }


}
