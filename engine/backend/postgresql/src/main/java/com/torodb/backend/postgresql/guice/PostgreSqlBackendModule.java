/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.postgresql.guice;

import com.google.inject.PrivateModule;
import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.MetaDataReadInterface;
import com.torodb.backend.MetaDataWriteInterface;
import com.torodb.backend.ReadInterface;
import com.torodb.backend.StructureInterface;
import com.torodb.backend.WriteInterface;
import com.torodb.backend.driver.postgresql.OfficialPostgreSqlDriver;
import com.torodb.backend.driver.postgresql.PostgreSqlDriverProvider;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.postgresql.PostgreSqlDataTypeProvider;
import com.torodb.backend.postgresql.PostgreSqlDbBackend;
import com.torodb.backend.postgresql.PostgreSqlErrorHandler;
import com.torodb.backend.postgresql.PostgreSqlIdentifierConstraints;
import com.torodb.backend.postgresql.PostgreSqlMetaDataReadInterface;
import com.torodb.backend.postgresql.PostgreSqlMetaDataWriteInterface;
import com.torodb.backend.postgresql.PostgreSqlMetrics;
import com.torodb.backend.postgresql.PostgreSqlReadInterface;
import com.torodb.backend.postgresql.PostgreSqlStructureInterface;
import com.torodb.backend.postgresql.PostgreSqlWriteInterface;
import com.torodb.backend.postgresql.meta.PostgreSqlSchemaUpdater;
import com.torodb.core.backend.IdentifierConstraints;

import javax.inject.Singleton;

public class PostgreSqlBackendModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(OfficialPostgreSqlDriver.class)
        .in(Singleton.class);
    bind(PostgreSqlDriverProvider.class)
        .to(OfficialPostgreSqlDriver.class);

    bind(PostgreSqlDbBackend.class)
        .in(Singleton.class);
    bind(DbBackendService.class)
        .to(PostgreSqlDbBackend.class);
    expose(DbBackendService.class);

    bind(PostgreSqlSchemaUpdater.class)
        .in(Singleton.class);
    bind(SchemaUpdater.class)
        .to(PostgreSqlSchemaUpdater.class);
    expose(SchemaUpdater.class);

    bind(PostgreSqlMetaDataReadInterface.class)
        .in(Singleton.class);
    bind(MetaDataReadInterface.class)
        .to(PostgreSqlMetaDataReadInterface.class);
    expose(MetaDataReadInterface.class);

    bind(PostgreSqlMetaDataWriteInterface.class)
        .in(Singleton.class);
    bind(MetaDataWriteInterface.class)
        .to(PostgreSqlMetaDataWriteInterface.class);
    expose(MetaDataWriteInterface.class);

    bind(PostgreSqlDataTypeProvider.class)
        .in(Singleton.class);
    bind(DataTypeProvider.class)
        .to(PostgreSqlDataTypeProvider.class);
    expose(DataTypeProvider.class);

    bind(PostgreSqlStructureInterface.class)
        .in(Singleton.class);
    bind(StructureInterface.class)
        .to(PostgreSqlStructureInterface.class);
    expose(StructureInterface.class);

    bind(PostgreSqlReadInterface.class)
        .in(Singleton.class);
    bind(ReadInterface.class)
        .to(PostgreSqlReadInterface.class);
    expose(ReadInterface.class);

    bind(PostgreSqlWriteInterface.class)
        .in(Singleton.class);
    bind(WriteInterface.class)
        .to(PostgreSqlWriteInterface.class);
    expose(WriteInterface.class);

    bind(PostgreSqlErrorHandler.class)
        .in(Singleton.class);
    bind(ErrorHandler.class)
        .to(PostgreSqlErrorHandler.class);
    expose(ErrorHandler.class);

    bind(PostgreSqlIdentifierConstraints.class)
        .in(Singleton.class);
    bind(IdentifierConstraints.class)
        .to(PostgreSqlIdentifierConstraints.class);
    expose(IdentifierConstraints.class);

    bind(PostgreSqlMetrics.class)
        .in(Singleton.class);
  }

}
