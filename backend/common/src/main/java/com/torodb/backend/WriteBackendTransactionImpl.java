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
import com.torodb.core.TableRef;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.*;
import java.sql.SQLException;

public class WriteBackendTransactionImpl extends BackendTransactionImpl implements WriteBackendTransaction {

    public WriteBackendTransactionImpl(SqlInterface sqlInterface, BackendConnectionImpl backendConnection,
            R2DTranslator r2dTranslator) {
        super(sqlInterface.getDbBackend().createWriteConnection(), sqlInterface, backendConnection, r2dTranslator);
    }
    
    @Override
    public void addDatabase(MetaDatabase db) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaDatabase(getDsl(), db.getName(), db.getIdentifier());
        getSqlInterface().getStructureInterface().createSchema(getDsl(), db.getIdentifier());
    }

    @Override
    public void addCollection(MetaDatabase db, MetaCollection newCol) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaCollection(getDsl(), db.getName(), newCol.getName(), newCol.getIdentifier());
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaDocPart(getDsl(), db.getName(), col.getName(),
                newDocPart.getTableRef(), newDocPart.getIdentifier());
    	
        TableRef tableRef = newDocPart.getTableRef();
    	if (tableRef.isRoot()) {
    	    getSqlInterface().getStructureInterface().createRootDocPartTable(getDsl(), db.getIdentifier(), newDocPart.getIdentifier(), tableRef);
    	} else {
            getSqlInterface().getStructureInterface().createDocPartTable(getDsl(), db.getIdentifier(), newDocPart.getIdentifier(), tableRef,
                    col.getMetaDocPartByTableRef(tableRef.getParent().get()).getIdentifier());
    	}
    }

    @Override
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField){
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaField(getDsl(), db.getName(), col.getName(), docPart.getTableRef(),
                newField.getName(), newField.getIdentifier(), newField.getType());
        getSqlInterface().getStructureInterface().addColumnToDocPartTable(getDsl(), db.getIdentifier(),
                docPart.getIdentifier(), newField.getIdentifier(), getSqlInterface().getDataTypeProvider().getDataType(newField.getType()));
    }

	@Override
	public void addScalar(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar newScalar) {
		Preconditions.checkState(!isClosed(), "This transaction is closed");

        getSqlInterface().getMetaDataWriteInterface().addMetaScalar(getDsl(), db.getName(), col.getName(), docPart.getTableRef(),
				newScalar.getIdentifier(), newScalar.getType());
		getSqlInterface().getStructureInterface().addColumnToDocPartTable(getDsl(), db.getIdentifier(), docPart.getIdentifier(),
		        newScalar.getIdentifier(), getSqlInterface().getDataTypeProvider().getDataType(newScalar.getType()));
	}
    
    @Override
    public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        return getSqlInterface().getMetaDataWriteInterface().consumeRids(getDsl(), db.getName(), col.getName(), docPart.getTableRef(), howMany);
    }

    @Override
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        getSqlInterface().getWriteInterface().insertDocPartData(getDsl(), db.getIdentifier(), data);
    }

    @Override
    public void commit() throws UserException, RollbackException {
        Preconditions.checkState(!isClosed(), "This transaction is closed");
        
        try {
            getConnection().commit();
        } catch (SQLException ex) {
            getSqlInterface().getErrorHandler().handleUserAndRetryException(Context.commit, ex);
        } finally {
            getDsl().configuration().connectionProvider().release(getConnection());
        }
    }

}
