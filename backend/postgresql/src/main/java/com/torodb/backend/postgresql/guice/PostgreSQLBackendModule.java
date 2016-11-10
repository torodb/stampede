/*
 * MongoWP - ToroDB-poc: Backend PostgreSQL
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.postgresql.guice;

import javax.inject.Singleton;

import com.google.inject.PrivateModule;
import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.MetaDataReadInterface;
import com.torodb.backend.MetaDataWriteInterface;
import com.torodb.backend.ReadInterface;
import com.torodb.backend.StructureInterface;
import com.torodb.backend.WriteInterface;
import com.torodb.backend.driver.postgresql.OfficialPostgreSQLDriver;
import com.torodb.backend.driver.postgresql.PostgreSQLDriverProvider;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.postgresql.*;
import com.torodb.backend.postgresql.meta.PostgreSQLSchemaUpdater;
import com.torodb.core.backend.IdentifierConstraints;

public class PostgreSQLBackendModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(OfficialPostgreSQLDriver.class)
                .in(Singleton.class);
        bind(PostgreSQLDriverProvider.class)
                .to(OfficialPostgreSQLDriver.class);

        bind(PostgreSQLDbBackend.class)
                .in(Singleton.class);
        bind(DbBackendService.class)
                .to(PostgreSQLDbBackend.class);
        expose(DbBackendService.class);

        bind(PostgreSQLSchemaUpdater.class)
                .in(Singleton.class);
        bind(SchemaUpdater.class)
                .to(PostgreSQLSchemaUpdater.class);
        expose(SchemaUpdater.class);

        bind(PostgreSQLMetaDataReadInterface.class)
                .in(Singleton.class);
        bind(MetaDataReadInterface.class)
                .to(PostgreSQLMetaDataReadInterface.class);
        expose(MetaDataReadInterface.class);

        bind(PostgreSQLMetaDataWriteInterface.class)
                .in(Singleton.class);
        bind(MetaDataWriteInterface.class)
                .to(PostgreSQLMetaDataWriteInterface.class);
        expose(MetaDataWriteInterface.class);

        bind(PostgreSQLDataTypeProvider.class)
                .in(Singleton.class);
        bind(DataTypeProvider.class)
                .to(PostgreSQLDataTypeProvider.class);
        expose(DataTypeProvider.class);

        bind(PostgreSQLStructureInterface.class)
                .in(Singleton.class);
        bind(StructureInterface.class)
                .to(PostgreSQLStructureInterface.class);
        expose(StructureInterface.class);

        bind(PostgreSQLReadInterface.class)
                .in(Singleton.class);
        bind(ReadInterface.class)
                .to(PostgreSQLReadInterface.class);
        expose(ReadInterface.class);

        bind(PostgreSQLWriteInterface.class)
                .in(Singleton.class);
        bind(WriteInterface.class)
                .to(PostgreSQLWriteInterface.class);
        expose(WriteInterface.class);

        bind(PostgreSQLErrorHandler.class)
                .in(Singleton.class);
        bind(ErrorHandler.class)
                .to(PostgreSQLErrorHandler.class);
        expose(ErrorHandler.class);

        bind(PostgreSQLIdentifierConstraints.class)
                .in(Singleton.class);
        bind(IdentifierConstraints.class)
                .to(PostgreSQLIdentifierConstraints.class);
        expose(IdentifierConstraints.class);

        bind(PostgreSQLMetrics.class)
                .in(Singleton.class);
    }

}
