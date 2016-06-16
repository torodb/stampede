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

import com.google.common.collect.ImmutableList;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import com.torodb.core.backend.WriteBackendTransaction;

public class BackendConnectionImpl implements WriteBackendTransaction {
    
    private final DSLContext dsl;
    private final DatabaseInterface databaseInterface;
    
    public BackendConnectionImpl(DSLContext dsl, DatabaseInterface databaseInterface) {
        super();
        this.dsl = dsl;
        this.databaseInterface = databaseInterface;
    }
    
    @Override
    public void addDatabase(MetaDatabase db) {
        MetaDatabaseTable<MetaDatabaseRecord> metaDatabaseTable = databaseInterface.getMetaDatabaseTable();
        dsl.insertInto(metaDatabaseTable)
            .set(metaDatabaseTable.newRecord()
            .values(db.getName(), db.getIdentifier()))
            .execute();
        dsl.execute(databaseInterface.createSchemaStatement(db.getIdentifier()));
    }

    @Override
    public void addCollection(MetaDatabase db, MetaCollection newCol) {
        MetaCollectionTable<MetaCollectionRecord> metaCollectionTable = databaseInterface.getMetaCollectionTable();
        dsl.insertInto(metaCollectionTable)
            .set(metaCollectionTable.newRecord()
            .values(db.getName(), newCol.getName(), newCol.getIdentifier()))
            .execute();
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
        MetaDocPartTable<Object, MetaDocPartRecord<Object>> metaDocPartTable = databaseInterface.getMetaDocPartTable();
        dsl.insertInto(metaDocPartTable)
            .set(metaDocPartTable.newRecord()
            .values(db.getName(), col.getName(), 
                    newDocPart.getTableRef(), newDocPart.getIdentifier()))
            .execute();
        ImmutableList.Builder<Field<?>> docPartFieldsBuilder = ImmutableList.<Field<?>>builder()
            .addAll(databaseInterface.getDocPartTableInternalFields(newDocPart));
        newDocPart.streamFields().forEach(field -> {
            docPartFieldsBuilder.add(DSL.field(field.getIdentifier(), databaseInterface.getDataType(field.getType())));
        });
        dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), db.getIdentifier(), 
            newDocPart.getIdentifier(), 
            docPartFieldsBuilder.build()));
    }

    @Override
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField){
        MetaFieldTable<Object, MetaFieldRecord<Object>> metaFieldTable = databaseInterface.getMetaFieldTable();
        dsl.insertInto(metaFieldTable)
            .set(metaFieldTable.newRecord()
            .values(db.getName(), col.getName(), docPart.getTableRef(), 
                    newField.getName(), newField.getIdentifier(), newField.getType()))
            .execute();
        dsl.execute(databaseInterface.addColumnToDocPartTableStatement(dsl.configuration(), db.getIdentifier(), 
                docPart.getIdentifier(),
                DSL.field(newField.getIdentifier(), databaseInterface.getDataType(newField.getType()))));
    }

    @Override
    public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany) {
        return databaseInterface.consumeRids(dsl, db.getName(), col.getName(), docPart.getTableRef(), howMany);
    }

    @Override
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) {
        databaseInterface.insertDocPartData(dsl, db.getIdentifier(), data);
    }

}
