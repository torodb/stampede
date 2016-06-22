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

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.DbBackend;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.MetaDataReadInterface;
import com.torodb.backend.MetaDataWriteInterface;
import com.torodb.backend.ReadInterface;
import com.torodb.backend.StructureInterface;
import com.torodb.backend.WriteInterface;
import com.torodb.backend.derby.DerbyDataTypeProvider;
import com.torodb.backend.derby.DerbyDbBackend;
import com.torodb.backend.derby.DerbyErrorHandler;
import com.torodb.backend.derby.DerbyMetaDataReadInterface;
import com.torodb.backend.derby.DerbyMetaDataWriteInterface;
import com.torodb.backend.derby.DerbyReadInterface;
import com.torodb.backend.derby.DerbyStructureInterface;
import com.torodb.backend.derby.DerbyWriteInterface;
import com.torodb.backend.driver.derby.DerbyDriverProvider;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;

public class DerbyBackendModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DerbyDriverProvider.class).to(OfficialDerbyDriver.class).in(Singleton.class);
        bind(DbBackend.class).to(DerbyDbBackend.class).in(Singleton.class);
        bind(MetaDataReadInterface.class).to(DerbyMetaDataReadInterface.class).in(Singleton.class);
        bind(MetaDataWriteInterface.class).to(DerbyMetaDataWriteInterface.class).in(Singleton.class);
        bind(DataTypeProvider.class).to(DerbyDataTypeProvider.class).in(Singleton.class);
        bind(StructureInterface.class).to(DerbyStructureInterface.class).in(Singleton.class);
        bind(ReadInterface.class).to(DerbyReadInterface.class).in(Singleton.class);
        bind(WriteInterface.class).to(DerbyWriteInterface.class).in(Singleton.class);
        bind(ErrorHandler.class).to(DerbyErrorHandler.class).in(Singleton.class);
    }

}
