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

package com.torodb.backend;

import java.sql.SQLException;

import org.junit.Before;

import com.google.inject.Injector;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;

public abstract class AbstractBackendTest {
    
    protected static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    
    protected TestSchema schema;
    protected SqlInterface sqlInterface;
    
    @Before
    public void setUp() throws Exception {
        Injector injector = createInjector();
        cleanDatabase(injector);
        sqlInterface = injector.getInstance(SqlInterface.class);
        schema = new TestSchema(tableRefFactory, sqlInterface);
    }

    protected abstract Injector createInjector();
    
    protected abstract void cleanDatabase(Injector injector) throws SQLException;
    
}
