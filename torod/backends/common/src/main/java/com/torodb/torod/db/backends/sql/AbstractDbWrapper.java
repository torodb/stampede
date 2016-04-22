/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.backends.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.collect.MapMaker;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbConnection.Metainfo;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.backends.meta.CollectionSchema;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.meta.routines.QueryRoutine;
import com.torodb.torod.db.backends.query.QueryEvaluator;

/**
 *
 */
public abstract class AbstractDbWrapper implements DbWrapper {
	private final AtomicBoolean isInitialized;
    private final DataSource sessionDataSource;
    private final DataSource systemDataSource;
    private final DataSource globalCursorDataSource;
    private final ConcurrentMap<CursorId, com.torodb.torod.core.dbWrapper.Cursor> openCursors;
    private final String databaseName;
    private TorodbMeta meta;
    private final DatabaseInterface databaseInterface;
    private final QueryRoutine queryRoutine;

    private static final DbConnection.Metainfo CURSOR_CONNECTION_METADATA = new Metainfo(true);
    
    @Inject
    public AbstractDbWrapper(
            @DatabaseName String databaseName,
            DbBackend dbBackend,
            QueryRoutine queryRoutine,
            DatabaseInterface databaseInterface
    ) {
        this.sessionDataSource = dbBackend.getSessionDataSource();
        this.systemDataSource = dbBackend.getSystemDataSource();
        this.globalCursorDataSource = dbBackend.getGlobalCursorDatasource();

        isInitialized = new AtomicBoolean(false);
        this.openCursors = new MapMaker().makeMap();
        this.databaseName = databaseName;
        this.queryRoutine = queryRoutine;
        this.databaseInterface = databaseInterface;
    }

    protected abstract Configuration getJooqConfiguration(ConnectionProvider connectionProvider);

    protected abstract DbConnection reserveConnection(DSLContext dsl, TorodbMeta meta);

    private DSLContext getDsl(Connection c) {
        return DSL.using(getJooqConfiguration(new MyConnectionProvider(c)));
    }

    protected abstract void checkDbSupported(Connection c) throws SQLException, ImplementationDbException;

    @Override
    public void initialize() throws ImplementationDbException {
        if (isInitialized()) {
            throw new IllegalStateException("The db-wrapper is already initialized");
        }

        try (Connection c = sessionDataSource.getConnection()) {
            try {
                checkDbSupported(c);
                c.setAutoCommit(false);

                meta = databaseInterface.initializeTorodbMeta(databaseName, getDsl(c), databaseInterface);
                c.commit();

                isInitialized.set(true);
            } catch (IOException ex) {
                //TODO: Change exception
                c.rollback();
                throw new RuntimeException(ex);
            } catch (DataAccessException ex) {
                //TODO: Change exception
                c.rollback();
                throw new RuntimeException(ex);
            } catch (InvalidDatabaseException ex) {
                //TODO: Change exception
                c.rollback();
                throw new RuntimeException(ex);
            }
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public DbConnection consumeSessionDbConnection(DbConnection.Metainfo metadata) {
        return createDbConnection(consumeConnection(sessionDataSource, metadata));
    }

    @Override
    public DbConnection getSystemDbConnection(DbConnection.Metainfo metadata) throws ImplementationDbException {
        return createDbConnection(consumeConnection(systemDataSource, metadata));
    }
    
    private DbConnection createDbConnection(Connection c) {
        return reserveConnection(getDsl(c), meta);
    }

    protected void postConsume(Connection c, DbConnection.Metainfo metadata) throws SQLException {
        c.setReadOnly(metadata.isReadOnly());
        if (!c.isValid(500)) {
            throw new RuntimeException("DB connection is not valid");
        }
        c.setAutoCommit(false);
    }
    
    private Connection consumeConnection(DataSource ds, DbConnection.Metainfo metadata) {
        if (!isInitialized()) {
            throw new IllegalStateException("The db-wrapper is not initialized");
        }
        try {
            Connection c = ds.getConnection();
            postConsume(c, metadata);

            return c;
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }
    
    
    public boolean isInitialized() {
        return isInitialized.get();
    }

    @Override
    public com.torodb.torod.core.dbWrapper.Cursor openGlobalCursor(
            String collection, 
            CursorId cursorId, 
            QueryCriteria filter, 
            Projection projection,
            int maxResults
    ) throws ImplementationDbException, UserDbException {
        
        com.torodb.torod.core.dbWrapper.Cursor cursor;

        if (!meta.exists(collection)) {
            cursor = new EmptyCursor();
        }
        else {
            CollectionSchema colSchema = meta.getCollectionSchema(collection);

            QueryEvaluator queryEvaluator = new QueryEvaluator(colSchema, databaseInterface);

            Connection connection = consumeConnection(globalCursorDataSource, CURSOR_CONNECTION_METADATA);
            DSLContext dsl = getDsl(connection);
            
            Set<Integer> dids = queryEvaluator.evaluateDid(
                    filter, 
                    dsl, 
                    maxResults
            );

            cursor = new DefaultCursor(
                    new AbstractDbWrapper.MyCursorConnectionProvider(
                            cursorId, 
                            dsl, 
                            colSchema, 
                            connection,
                            projection
                    ),
                    dids
            );
        }

        openCursors.put(cursorId, cursor);

        return cursor;
    }

    @Override
    public com.torodb.torod.core.dbWrapper.Cursor getGlobalCursor(CursorId cursorId) 
            throws IllegalArgumentException {
        com.torodb.torod.core.dbWrapper.Cursor cursor = openCursors.get(cursorId);
        if (cursor == null) {
            throw new IllegalArgumentException("There is no open cursor with id " + cursorId);
        }

        return cursor;
    }

    public static class MyConnectionProvider implements ConnectionProvider {

        private final Connection connection;

        public MyConnectionProvider(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection acquire() throws DataAccessException {
            return connection;
        }

        @Override
        public void release(Connection connection) throws DataAccessException {
        }

    }

    class MyCursorConnectionProvider implements DefaultCursor.DatabaseCursorGateway {

        private final CursorId cursorId;
        private final Configuration configuration;
        private final CollectionSchema colSchema;
        private final Connection connection;
        private final Projection projection;

        public MyCursorConnectionProvider(
                CursorId cursorId, 
                DSLContext dsl, 
                CollectionSchema colSchema,
                Connection connection,
                Projection projection) {
            this.cursorId = cursorId;
            this.configuration = dsl.configuration();
            this.colSchema = colSchema;
            this.connection = connection;
            this.projection = projection;
            
            assert configuration.connectionProvider().acquire() == connection;
        }

        @Override
        public List<SplitDocument> readDocuments(Integer[] documents) {
            return queryRoutine.execute(configuration, colSchema, documents, projection, databaseInterface);
        }

        @Override
        public void close() throws SQLException {
            connection.commit();
            connection.close();
            openCursors.remove(cursorId);
        }

    }

}
