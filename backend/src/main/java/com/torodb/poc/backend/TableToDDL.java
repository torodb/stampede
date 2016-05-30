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

package com.torodb.poc.backend;

import java.io.Serializable;
import java.util.Collection;

import javax.inject.Inject;

import org.jooq.DSLContext;
import org.jooq.DataType;

import com.torodb.kvdocument.values.KVValue;
import com.torodb.poc.backend.mocks.ImplementationDbException;
import com.torodb.poc.backend.mocks.Path;
import com.torodb.poc.backend.mocks.RetryTransactionException;

public class TableToDDL {
    private final DatabaseInterface databaseInterface;
    
    @Inject
    public TableToDDL(DatabaseInterface databaseInterface) {
        super();
        this.databaseInterface = databaseInterface;
    }

    public void insert(DSLContext dsl, CollectionSnapshot collectionSnapshot, TableBulk tableBulk) throws ImplementationDbException, RetryTransactionException {
        String schema = collectionSnapshot.getDatabaseSnapshot().getSchemaName();
        for (TableData tableData : tableBulk) {
            PathSnapshot pathSnapshot = collectionSnapshot.getPathSnapshot(tableData.getPath());
            databaseInterface.insertPathDocuments(dsl, schema, pathSnapshot, tableData);
        }
    }
    
    public interface DatabaseSnapshot {
        public String getSchemaName();
        public CollectionSnapshot getCollectionSnapshot(String collectionName);
    }
    
    public interface CollectionSnapshot {
        public DatabaseSnapshot getDatabaseSnapshot();
        public PathSnapshot getPathSnapshot(Path path);
    }
    
    public interface PathSnapshot {
        public String getTableName();
        public String getColumnName(String name);
    }
    
    public interface FieldSnapshot {
        public String getColumnName();
        public DataType<?> getColumnType();
    }
    
    public interface TableBulk extends Iterable<TableData> {
    }
    
    public interface TableData extends Collection<TableRow> {
        public Path getPath();
    }
    
    public interface TableRow extends Collection<TableColumn> {
        public int getDid();
        public int getRid();
        public Integer getPid();
        public Integer getSeq();
    }
    
    public interface TableColumn {
        public String getName();
        public KVValue<? extends Serializable> getValue();
    }
    
}
