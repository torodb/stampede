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

import org.jooq.Field;
import org.junit.Before;
import org.junit.ClassRule;

import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.meta.SnapshotUpdater;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

public abstract class AbstractBackendTest {

    @ClassRule
    public final static BackendRunnerClassRule BACKEND_RUNNER_CLASS_RULE = new BackendRunnerClassRule();
    
    protected static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    
    protected TestSchema schema;
    protected SchemaUpdater schemaUpdater;
    protected SqlInterface sqlInterface;
    protected SqlHelper sqlHelper;
    
    @Before
    public void setUp() throws Exception {
        schemaUpdater = BACKEND_RUNNER_CLASS_RULE.getSchemaUpdater();
        sqlInterface = BACKEND_RUNNER_CLASS_RULE.getSqlInterface();
        sqlHelper = BACKEND_RUNNER_CLASS_RULE.getSqlHelper();
        schema = new TestSchema(tableRefFactory, sqlInterface);
        BACKEND_RUNNER_CLASS_RULE.cleanDatabase();
    }

    protected FieldType fieldType(Field<?> field) {
        return FieldType.from(((DataTypeForKV<?>) field.getDataType()).getKVValueConverter().getErasuredType());
    }

    protected ImmutableMetaSnapshot buildMetaSnapshot() {
        MvccMetainfoRepository metainfoRepository = new MvccMetainfoRepository();
        SnapshotUpdater.updateSnapshot(metainfoRepository, sqlInterface, sqlHelper, schemaUpdater, tableRefFactory);

        try (SnapshotStage stage = metainfoRepository.startSnapshotStage()) {
            return stage.createImmutableSnapshot();
        }
    }

    protected TableRef createTableRef(String...names) {
        TableRef tableRef = tableRefFactory.createRoot();
        
        for (String name : names) {
            try {
                int index = Integer.parseInt(name);
                tableRef = tableRefFactory.createChild(tableRef, index);
            } catch(NumberFormatException ex) {
                tableRef = tableRefFactory.createChild(tableRef, name);
            }
        }
        
        return tableRef;
    }
}
