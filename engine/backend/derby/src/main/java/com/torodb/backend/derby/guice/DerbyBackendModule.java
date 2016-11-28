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

package com.torodb.backend.derby.guice;

import com.google.inject.PrivateModule;
import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.MetaDataReadInterface;
import com.torodb.backend.MetaDataWriteInterface;
import com.torodb.backend.ReadInterface;
import com.torodb.backend.StructureInterface;
import com.torodb.backend.WriteInterface;
import com.torodb.backend.derby.DerbyDataTypeProvider;
import com.torodb.backend.derby.DerbyDbBackend;
import com.torodb.backend.derby.DerbyErrorHandler;
import com.torodb.backend.derby.DerbyIdentifierConstraints;
import com.torodb.backend.derby.DerbyMetaDataReadInterface;
import com.torodb.backend.derby.DerbyMetaDataWriteInterface;
import com.torodb.backend.derby.DerbyReadInterface;
import com.torodb.backend.derby.DerbyStructureInterface;
import com.torodb.backend.derby.DerbyWriteInterface;
import com.torodb.backend.derby.schema.DerbySchemaUpdater;
import com.torodb.backend.driver.derby.DerbyDriverProvider;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.backend.IdentifierConstraints;

import javax.inject.Singleton;

public class DerbyBackendModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(OfficialDerbyDriver.class)
        .in(Singleton.class);
    bind(DerbyDriverProvider.class)
        .to(OfficialDerbyDriver.class);

    bind(DerbyDbBackend.class)
        .in(Singleton.class);
    bind(DbBackendService.class)
        .to(DerbyDbBackend.class);
    expose(DbBackendService.class);

    bind(DerbySchemaUpdater.class)
        .in(Singleton.class);
    bind(SchemaUpdater.class)
        .to(DerbySchemaUpdater.class);
    expose(SchemaUpdater.class);

    bind(DerbyMetaDataReadInterface.class)
        .in(Singleton.class);
    bind(MetaDataReadInterface.class)
        .to(DerbyMetaDataReadInterface.class);
    expose(MetaDataReadInterface.class);

    bind(DerbyMetaDataWriteInterface.class)
        .in(Singleton.class);
    bind(MetaDataWriteInterface.class)
        .to(DerbyMetaDataWriteInterface.class);
    expose(MetaDataWriteInterface.class);

    bind(DerbyDataTypeProvider.class)
        .in(Singleton.class);
    bind(DataTypeProvider.class)
        .to(DerbyDataTypeProvider.class);
    expose(DataTypeProvider.class);

    bind(DerbyStructureInterface.class)
        .in(Singleton.class);
    bind(StructureInterface.class)
        .to(DerbyStructureInterface.class);
    expose(StructureInterface.class);

    bind(DerbyReadInterface.class)
        .in(Singleton.class);
    bind(ReadInterface.class)
        .to(DerbyReadInterface.class);
    expose(ReadInterface.class);

    bind(DerbyWriteInterface.class)
        .in(Singleton.class);
    bind(WriteInterface.class)
        .to(DerbyWriteInterface.class);
    expose(WriteInterface.class);

    bind(DerbyErrorHandler.class)
        .in(Singleton.class);
    bind(ErrorHandler.class)
        .to(DerbyErrorHandler.class);
    expose(ErrorHandler.class);

    bind(DerbyIdentifierConstraints.class)
        .in(Singleton.class);
    bind(IdentifierConstraints.class)
        .to(DerbyIdentifierConstraints.class);
    expose(IdentifierConstraints.class);
  }

}
