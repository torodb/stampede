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
import java.util.Iterator;

import org.jooq.DSLContext;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.TableRef;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;

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
    public void dropCollection(MetaDatabase db, MetaCollection coll) {
        Preconditions.checkState(!closed, "This transaction is closed");

        Iterator<? extends MetaDocPart> metaDocPartIterator = coll.streamContainedMetaDocParts().iterator();
        while (metaDocPartIterator.hasNext()) {
            MetaDocPart metaDocPart = metaDocPartIterator.next();
            dropMetaDocPart(db, coll, metaDocPart);
        }
        sqlInterface.getMetaDataWriteInterface().deleteMetaCollection(dsl, db.getName(), coll.getName());
        sqlInterface.getStructureInterface().dropCollection(dsl, db.getIdentifier(), coll);
    }

    private void dropMetaDocPart(MetaDatabase db, MetaCollection coll, MetaDocPart metaDocPart) {
        dropMetaScalars(db, coll, metaDocPart);
        dropMetaFields(db, coll, metaDocPart);
        sqlInterface.getMetaDataWriteInterface().deleteMetaDocPart(dsl, db.getName(), coll.getName(), 
                metaDocPart.getTableRef());
    }

    private void dropMetaScalars(MetaDatabase db, MetaCollection coll, MetaDocPart metaDocPart) {
        Iterator<? extends MetaScalar> metaScalarIterator = metaDocPart.streamScalars().iterator();
        while (metaScalarIterator.hasNext()) {
            MetaScalar metaScalar = metaScalarIterator.next();
            sqlInterface.getMetaDataWriteInterface().deleteMetaScalar(dsl, db.getName(), coll.getName(), 
                    metaDocPart.getTableRef(), metaScalar.getType());
        }
    }

    private void dropMetaFields(MetaDatabase db, MetaCollection coll, MetaDocPart metaDocPart) {
        Iterator<? extends MetaField> metaFieldIterator = metaDocPart.streamFields().iterator();
        while (metaFieldIterator.hasNext()) {
            MetaField metaField = metaFieldIterator.next();
            sqlInterface.getMetaDataWriteInterface().deleteMetaField(dsl, db.getName(), coll.getName(), 
                    metaDocPart.getTableRef(), metaField.getName(), metaField.getType());
        }
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dsl, db.getName(), col.getName(),
                newDocPart.getTableRef(), newDocPart.getIdentifier());
    	
    	TableRef tableRef = newDocPart.getTableRef();
    	if (tableRef.isRoot()) {
    	    sqlInterface.getStructureInterface().createRootDocPartTable(dsl, db.getIdentifier(), newDocPart.getIdentifier(), tableRef);
    	} else {
            sqlInterface.getStructureInterface().createDocPartTable(dsl, db.getIdentifier(), newDocPart.getIdentifier(), tableRef, 
                    col.getMetaDocPartByTableRef(tableRef.getParent().get()).getIdentifier());
    	}
    }

    @Override
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField){
        Preconditions.checkState(!closed, "This transaction is closed");

    	sqlInterface.getMetaDataWriteInterface().addMetaField(dsl, db.getName(), col.getName(), docPart.getTableRef(),
                newField.getName(), newField.getIdentifier(), newField.getType());
        sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, db.getIdentifier(),
                docPart.getIdentifier(), newField.getIdentifier(), sqlInterface.getDataTypeProvider().getDataType(newField.getType()));
    }

	@Override
	public void addScalar(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar newScalar) {
		Preconditions.checkState(!closed, "This transaction is closed");

		sqlInterface.getMetaDataWriteInterface().addMetaScalar(dsl, db.getName(), col.getName(), docPart.getTableRef(), 
				newScalar.getIdentifier(), newScalar.getType());
		sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, db.getIdentifier(), docPart.getIdentifier(), 
		        newScalar.getIdentifier(), sqlInterface.getDataTypeProvider().getDataType(newScalar.getType()));
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
            sqlInterface.getErrorHandler().handleUserAndRetryException(Context.COMMIT, ex);
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
                sqlInterface.getErrorHandler().handleRollbackException(Context.CLOSE, ex);
            }
            dsl.close();
        }
    }

}
