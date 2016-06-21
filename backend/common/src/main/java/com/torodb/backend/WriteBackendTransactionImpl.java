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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.torodb.backend.interfaces.ErrorHandlerInterface.Context;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class WriteBackendTransactionImpl implements WriteBackendTransaction {

    private boolean closed = false;
    private final Connection connection;
    private final DSLContext dsl;
    private final SqlInterface sqlInterface;
    
    public WriteBackendTransactionImpl(SqlInterface sqlInterface) {
        this.sqlInterface = sqlInterface;
        this.connection = sqlInterface.createWriteConnection();
        this.dsl = sqlInterface.createDSLContext(connection);
    }
    
    @Override
    public void addDatabase(MetaDatabase db) {
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.addMetaDatabase(dsl, db.getName(), db.getIdentifier());
        sqlInterface.createSchema(dsl, db.getIdentifier());
    }

    @Override
    public void addCollection(MetaDatabase db, MetaCollection newCol) {
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.addMetaCollection(dsl, db.getName(), newCol.getName(), newCol.getIdentifier());
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.addMetaDocPart(dsl, db.getName(), col.getName(),
                newDocPart.getTableRef(), newDocPart.getIdentifier());
    	
        ImmutableList.Builder<Field<?>> docPartFieldsBuilder = ImmutableList.<Field<?>>builder()
            .addAll(sqlInterface.getDocPartTableInternalFields(newDocPart));
        newDocPart.streamFields().map(this::buildField).forEach(docPartFieldsBuilder::add);
        List<Field<?>> fields = docPartFieldsBuilder.build();
        sqlInterface.createDocPartTable(dsl, db.getIdentifier(), newDocPart.getIdentifier(), fields);
    }

    @Override
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField){
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.addMetaField(dsl, db.getName(), col.getName(), docPart.getTableRef(),
                newField.getName(), newField.getIdentifier(), newField.getType());
        sqlInterface.addColumnToDocPartTable(dsl, db.getIdentifier(),
                docPart.getIdentifier(),buildField(newField));
    }

    @Override
    public void addScalar(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar newScalar) {
        Preconditions.checkState(!closed, "This transaction is closed");
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement
    }

	private Field<?> buildField(MetaField newField) {
        Preconditions.checkState(!closed, "This transaction is closed");

		return DSL.field(newField.getIdentifier(), sqlInterface.getDataType(newField.getType()));
	}

    @Override
    public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany) {
        Preconditions.checkState(!closed, "This transaction is closed");

        return sqlInterface.consumeRids(dsl, db.getName(), col.getName(), docPart.getTableRef(), howMany);
    }

    @Override
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) {
        Preconditions.checkState(!closed, "This transaction is closed");
        
        sqlInterface.insertDocPartData(dsl, db.getIdentifier(), data);
    }

    @Override
    public void commit() throws UserException, RollbackException {
        Preconditions.checkState(!closed, "This transaction is closed");
        
        try {
            connection.commit();
        } catch (SQLException ex) {
            sqlInterface.handleUserAndRetryException(Context.commit, ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                connection.close();
            } catch (SQLException ex) {
                sqlInterface.handleRollbackException(Context.close, ex);
            }
            dsl.close();
        }
    }

}
