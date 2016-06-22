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

package com.torodb.backend.postgresql.guice;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.DbBackend;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.MetaDataReadInterface;
import com.torodb.backend.MetaDataWriteInterface;
import com.torodb.backend.ReadInterface;
import com.torodb.backend.StructureInterface;
import com.torodb.backend.WriteInterface;
import com.torodb.backend.driver.postgresql.OfficialPostgreSQLDriver;
import com.torodb.backend.driver.postgresql.PostgreSQLDriverProvider;
import com.torodb.backend.postgresql.PostgreSQLDataTypeProvider;
import com.torodb.backend.postgresql.PostgreSQLDbBackend;
import com.torodb.backend.postgresql.PostgreSQLErrorHandler;
import com.torodb.backend.postgresql.PostgreSQLIdentifierConstraints;
import com.torodb.backend.postgresql.PostgreSQLMetaDataReadInterface;
import com.torodb.backend.postgresql.PostgreSQLMetaDataWriteInterface;
import com.torodb.backend.postgresql.PostgreSQLReadInterface;
import com.torodb.backend.postgresql.PostgreSQLStructureInterface;
import com.torodb.backend.postgresql.PostgreSQLWriteInterface;
import com.torodb.core.backend.IdentifierConstraints;

public class PostgreSQLBackendModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(PostgreSQLDriverProvider.class).to(OfficialPostgreSQLDriver.class).in(Singleton.class);
        bind(PostgreSQLDbBackend.class).in(Singleton.class);
        bind(DbBackend.class).to(PostgreSQLDbBackend.class);
        bind(DbBackendService.class).to(PostgreSQLDbBackend.class);
        bind(MetaDataReadInterface.class).to(PostgreSQLMetaDataReadInterface.class).in(Singleton.class);
        bind(MetaDataWriteInterface.class).to(PostgreSQLMetaDataWriteInterface.class).in(Singleton.class);
        bind(DataTypeProvider.class).to(PostgreSQLDataTypeProvider.class).in(Singleton.class);
        bind(StructureInterface.class).to(PostgreSQLStructureInterface.class).in(Singleton.class);
        bind(ReadInterface.class).to(PostgreSQLReadInterface.class).in(Singleton.class);
        bind(WriteInterface.class).to(PostgreSQLWriteInterface.class).in(Singleton.class);
        bind(ErrorHandler.class).to(PostgreSQLErrorHandler.class).in(Singleton.class);
        bind(IdentifierConstraints.class).to(PostgreSQLIdentifierConstraints.class).in(Singleton.class);
    }

}
