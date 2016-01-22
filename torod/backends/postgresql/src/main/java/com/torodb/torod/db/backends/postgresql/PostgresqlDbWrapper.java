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


package com.torodb.torod.db.backends.postgresql;

import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocType.Builder;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.meta.routines.QueryRoutine;
import com.torodb.torod.db.backends.sql.AbstractDbWrapper;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DefaultConfiguration;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import javax.inject.Provider;

/**
 *
 */
@Singleton
public class PostgresqlDbWrapper extends AbstractDbWrapper {
    private static final int DB_SUPPORT_MAJOR = 9;
    private static final int DB_SUPPORT_MINOR = 4;

    private final DatabaseInterface databaseInterface;
    private final Provider<SubDocType.Builder> subDocTypeBuilderProvider;

    @Inject
    public PostgresqlDbWrapper(
            @DatabaseName String databaseName,
            DbBackend dbBackend,
            QueryRoutine queryRoutine,
            DatabaseInterface databaseInterface,
            Provider<SubDocType.Builder> subDocTypeBuilderProvider
    ) {
        super(databaseName, dbBackend, queryRoutine, databaseInterface);
        this.databaseInterface = databaseInterface;
        this.subDocTypeBuilderProvider = subDocTypeBuilderProvider;
    }

    @Override
    protected Configuration getJooqConfiguration(ConnectionProvider cp) {
        return new DefaultConfiguration()
                .set(cp)
                .set(SQLDialect.POSTGRES)
                .set(new Settings()
                        .withRenderNameStyle(RenderNameStyle.QUOTED)
                );
    }

    @Override
    protected DbConnection reserveConnection(DSLContext dsl, TorodbMeta meta) {
        return new PostgresqlDbConnection(dsl, meta, subDocTypeBuilderProvider, databaseInterface);
    }
    
    @Override
    protected void checkDbSupported(Connection conn) throws SQLException, ImplementationDbException {    	
        int major = conn.getMetaData().getDatabaseMajorVersion();
        int minor = conn.getMetaData().getDatabaseMinorVersion();
        
		if (! (major > DB_SUPPORT_MAJOR || (major == DB_SUPPORT_MAJOR && minor >= DB_SUPPORT_MINOR))) {
			throw new ImplementationDbException(
                    true,
                    "ToroDB requires PostgreSQL version " + DB_SUPPORT_MAJOR + "." + DB_SUPPORT_MINOR
                            +" or higher! Detected " + major + "." + minor
            );
		}
    }
}
