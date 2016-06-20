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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableList;
import com.torodb.backend.interfaces.ErrorHandlerInterface.Context;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;

public class WriteBackendTransactionImpl implements WriteBackendTransaction {
    
    private final DSLContext dsl;
    private final DatabaseInterface databaseInterface;
    
    public WriteBackendTransactionImpl(DSLContext dsl, DatabaseInterface databaseInterface) {
        super();
        this.dsl = dsl;
        this.databaseInterface = databaseInterface;
    }
    
    @Override
    public void addDatabase(MetaDatabase db) {
    	databaseInterface.addMetaDatabase(dsl, db.getName(), db.getIdentifier());
        databaseInterface.createSchema(dsl, db.getIdentifier());
    }

    @Override
    public void addCollection(MetaDatabase db, MetaCollection newCol) {
    	databaseInterface.addMetaCollection(dsl, db.getName(), newCol.getName(), newCol.getIdentifier());
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
    	databaseInterface.addMetaDocPart(dsl, db.getName(), col.getName(), 
                newDocPart.getTableRef(), newDocPart.getIdentifier());
    	
        ImmutableList.Builder<Field<?>> docPartFieldsBuilder = ImmutableList.<Field<?>>builder()
            .addAll(databaseInterface.getDocPartTableInternalFields(newDocPart));
        newDocPart.streamFields().map(this::buildField).forEach(docPartFieldsBuilder::add);
        List<Field<?>> fields = docPartFieldsBuilder.build();
        databaseInterface.createDocPartTable(dsl, db.getIdentifier(), newDocPart.getIdentifier(), fields);
    }

    @Override
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField){
    	databaseInterface.addMetaField(dsl, db.getName(), col.getName(), docPart.getTableRef(), 
                newField.getName(), newField.getIdentifier(), newField.getType());
        databaseInterface.addColumnToDocPartTable(dsl, db.getIdentifier(), 
                docPart.getIdentifier(),buildField(newField));
    }

	private Field<?> buildField(MetaField newField) {
		return DSL.field(newField.getIdentifier(), databaseInterface.getDataType(newField.getType()));
	}

    @Override
    public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany) {
        return databaseInterface.consumeRids(dsl, db.getName(), col.getName(), docPart.getTableRef(), howMany);
    }

    @Override
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) {
        databaseInterface.insertDocPartData(dsl, db.getIdentifier(), data);
    }

    @Override
    public void commit() throws UserException, RollbackException {
        Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
            connection.commit();
        } catch(SQLException ex) {
            databaseInterface.handleRollbackException(Context.commit, ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    @Override
    public void close() {
        dsl.close();
    }

}
