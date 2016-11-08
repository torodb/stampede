/*
 * MongoWP - ToroDB-poc: Integration Tests
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
package com.torodb.integration.backend;

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
        getService().startBackendBundle();
        sqlInterface = getService().getInjector().getInstance(SqlInterface.class);
        sqlHelper = getService().getInjector().getInstance(SqlHelper.class);
        schemaUpdater = getService().getInjector().getInstance(SchemaUpdater.class);
    }

    @Override
    protected void shutDown() throws Exception {
        if (getService() != null) {
            getService().shutDown();
        }
    }
    
    public void cleanDatabase() throws Exception {
        super.cleanDatabase();
    }
}
