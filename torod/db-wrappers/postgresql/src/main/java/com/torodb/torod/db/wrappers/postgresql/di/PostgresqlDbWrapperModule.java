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
package com.torodb.torod.db.wrappers.postgresql.di;


import com.google.inject.AbstractModule;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.db.wrappers.DatabaseInterface;
import com.torodb.torod.db.wrappers.converters.BasicTypeToSqlType;
import com.torodb.torod.db.wrappers.postgresql.PostgresqlDatabaseInterface;
import com.torodb.torod.db.wrappers.postgresql.PostgresqlDbWrapper;
import com.torodb.torod.db.wrappers.postgresql.converters.PostgresBasicTypeToSqlType;
import com.torodb.torod.db.wrappers.postgresql.driver.OfficialPostgreSQLDriver;
import com.torodb.torod.db.wrappers.postgresql.driver.PostgreSQLDriverProvider;

import javax.inject.Singleton;

/**
 *
 */
public class PostgresqlDbWrapperModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DbWrapper.class).to(PostgresqlDbWrapper.class).in(Singleton.class);
        bind(PostgresqlDbWrapper.class).in(Singleton.class);
        bind(DatabaseInterface.class).to(PostgresqlDatabaseInterface.class).in(Singleton.class);
        bind(BasicTypeToSqlType.class).to(PostgresBasicTypeToSqlType.class).in(Singleton.class);
        bind(PostgreSQLDriverProvider.class).to(OfficialPostgreSQLDriver.class).in(Singleton.class);
    }
}
