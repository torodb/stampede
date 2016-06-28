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
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class WriteBackendTransactionImpl implements WriteBackendTransaction {

    private boolean closed = false;
    private final Connection connection;
    private final DSLContext dsl;
    private final SqlInterface sqlInterface;
    private final BackendConnectionImpl backendConnection;
    
    public WriteBackendTransactionImpl(SqlInterface sqlInterface, BackendConnectionImpl backendConnection) {
        this.sqlInterface = sqlInterface;
        this.connection = sqlInterface.getDbBackend().createWriteConnection();
        this.dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
        this.backendConnection = backendConnection;
    }
    
    @Override
    public void addDatabase(MetaDatabase db) {
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.getMetaDataWriteInterface().addMetaDatabase(dsl, db.getName(), db.getIdentifier());
        sqlInterface.getStructureInterface().createSchema(dsl, db.getIdentifier());
    }

    @Override
    public void addCollection(MetaDatabase db, MetaCollection newCol) {
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.getMetaDataWriteInterface().addMetaCollection(dsl, db.getName(), newCol.getName(), newCol.getIdentifier());
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dsl, db.getName(), col.getName(),
                newDocPart.getTableRef(), newDocPart.getIdentifier());
    	
        Collection<? extends Field<?>> fields = sqlInterface.getMetaDataReadInterface().getDocPartTableInternalFields(newDocPart);
        sqlInterface.getStructureInterface().createDocPartTable(dsl, db.getIdentifier(), newDocPart.getIdentifier(), fields);
    }

    @Override
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField){
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.getMetaDataWriteInterface().addMetaField(dsl, db.getName(), col.getName(), docPart.getTableRef(),
                newField.getName(), newField.getIdentifier(), newField.getType());
        sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, db.getIdentifier(),
                docPart.getIdentifier(),buildField(newField));
    }

	@Override
	public void addScalar(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar newScalar) {
		Preconditions.checkState(!closed, "This transaction is closed");

		sqlInterface.getMetaDataWriteInterface().addMetaScalar(dsl, db.getName(), col.getName(), docPart.getTableRef(), 
				newScalar.getIdentifier(), newScalar.getType());
		sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, db.getIdentifier(), docPart.getIdentifier(), 
				buildScalar(newScalar));
	}
	
	private Field<?> buildField(MetaField newField) {
		return DSL.field(newField.getIdentifier(), sqlInterface.getDataTypeProvider().getDataType(newField.getType()));
	}
	
	private Field<?> buildScalar(MetaScalar newScalar) {
		return DSL.field(newScalar.getIdentifier(), sqlInterface.getDataTypeProvider().getDataType(newScalar.getType()));
	}

    @Override
    public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany) {
        Preconditions.checkState(!closed, "This transaction is closed");

        return sqlInterface.getMetaDataWriteInterface().consumeRids(dsl, db.getName(), col.getName(), docPart.getTableRef(), howMany);
    }

    @Override
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) {
        Preconditions.checkState(!closed, "This transaction is closed");
        
        sqlInterface.getWriteInterface().insertDocPartData(dsl, db.getIdentifier(), data);
    }

    @Override
    public void commit() throws UserException, RollbackException {
        Preconditions.checkState(!closed, "This transaction is closed");
        
        try {
            connection.commit();
        } catch (SQLException ex) {
            sqlInterface.getErrorHandler().handleUserAndRetryException(Context.commit, ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            backendConnection.onTransactionClosed(this);
            try {
                connection.rollback();
                connection.close();
            } catch (SQLException ex) {
                sqlInterface.getErrorHandler().handleRollbackException(Context.close, ex);
            }
            dsl.close();
        }
    }

}
