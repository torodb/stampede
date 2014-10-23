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


package com.torodb.torod.db.postgresql;

import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import com.torodb.torod.db.sql.AbstractSqlDbWrapper;
import com.torodb.torod.core.config.TorodConfig;
import com.torodb.torod.core.dbWrapper.DbConnection;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;

/**
 *
 */
@Singleton
public class PostgresqlDbWrapper extends AbstractSqlDbWrapper {

    @Inject
    public PostgresqlDbWrapper(TorodConfig config) {
        super(config);
    }

    @Override
    protected Configuration getJooqConfiguration(ConnectionProvider cp) {
        return new DefaultConfiguration()
                .set(cp)
                .set(SQLDialect.POSTGRES);
    }

    @Override
    protected DbConnection reserveConnection(DSLContext dsl, TorodbMeta meta) {
        return new PostgresqlDbConnection(dsl, meta);
    }
}
