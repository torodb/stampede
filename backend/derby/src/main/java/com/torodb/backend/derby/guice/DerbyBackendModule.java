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

package com.torodb.backend.derby.guice;

import com.google.inject.PrivateModule;
import com.torodb.backend.*;
import com.torodb.backend.derby.*;
import com.torodb.backend.derby.schema.DerbySchemaUpdater;
import com.torodb.backend.driver.derby.DerbyDriverProvider;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.backend.IdentifierConstraints;
import javax.inject.Singleton;

public class DerbyBackendModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(DerbyDriverProvider.class).to(OfficialDerbyDriver.class).in(Singleton.class);
        bind(DerbyDbBackend.class).in(Singleton.class);
        bind(DbBackendService.class).to(DerbyDbBackend.class);
        expose(DbBackendService.class);
        bind(SchemaUpdater.class).to(DerbySchemaUpdater.class).in(Singleton.class);
        expose(SchemaUpdater.class);
        bind(MetaDataReadInterface.class).to(DerbyMetaDataReadInterface.class).in(Singleton.class);
        expose(MetaDataReadInterface.class);
        bind(MetaDataWriteInterface.class).to(DerbyMetaDataWriteInterface.class).in(Singleton.class);
        expose(MetaDataWriteInterface.class);
        bind(DataTypeProvider.class).to(DerbyDataTypeProvider.class).in(Singleton.class);
        expose(DataTypeProvider.class);
        bind(StructureInterface.class).to(DerbyStructureInterface.class).in(Singleton.class);
        expose(StructureInterface.class);
        bind(ReadInterface.class).to(DerbyReadInterface.class).in(Singleton.class);
        expose(ReadInterface.class);
        bind(WriteInterface.class).to(DerbyWriteInterface.class).in(Singleton.class);
        expose(WriteInterface.class);
        bind(ErrorHandler.class).to(DerbyErrorHandler.class).in(Singleton.class);
        expose(ErrorHandler.class);
        bind(IdentifierConstraints.class).to(DerbyIdentifierConstraints.class).in(Singleton.class);
        expose(IdentifierConstraints.class);
    }

}
