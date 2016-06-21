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
import com.torodb.backend.DbBackend;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.derby.DerbyDbBackend;
import com.torodb.backend.derby.DerbySqlInterface;
import com.torodb.backend.driver.derby.DerbyDriverProvider;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.core.backend.IdentifierConstraints;

public class DerbyBackendModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DerbyDriverProvider.class).to(OfficialDerbyDriver.class).in(Singleton.class);
        bind(DbBackend.class).to(DerbyDbBackend.class).in(Singleton.class);
        bind(SqlInterface.class).to(DerbySqlInterface.class).in(Singleton.class);
        bind(IdentifierConstraints.class).to(DerbySqlInterface.class).in(Singleton.class);
    }

}
