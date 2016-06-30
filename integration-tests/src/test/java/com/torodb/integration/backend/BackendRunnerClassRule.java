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

package com.torodb.integration.backend;

import com.torodb.backend.DbBackendService;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.integration.AbstractBackendRunnerClassRule;

public class BackendRunnerClassRule extends AbstractBackendRunnerClassRule {

    private SchemaUpdater schemaUpdater;
    private SqlInterface sqlInterface;
    private SqlHelper sqlHelper;
    
    public SchemaUpdater getSchemaUpdater() {
        return schemaUpdater;
    }

    public SqlInterface getSqlInterface() {
        return sqlInterface;
    }

    public SqlHelper getSqlHelper() {
        return sqlHelper;
    }

    @Override
    protected void startUp() throws Exception {
        DbBackendService dbBackendService = getInjector().getInstance(DbBackendService.class);
        dbBackendService.startAsync();
        dbBackendService.awaitRunning();
        sqlInterface = getInjector().getInstance(SqlInterface.class);
        sqlHelper = getInjector().getInstance(SqlHelper.class);
        schemaUpdater = getInjector().getInstance(SchemaUpdater.class);
    }

    @Override
    protected void shutDown() throws Exception {
        DbBackendService dbBackendService = getInjector().getInstance(DbBackendService.class);
        dbBackendService.stopAsync();
        dbBackendService.awaitTerminated();
    }
    
    public void cleanDatabase() throws Exception {
        super.cleanDatabase();
    }
}
