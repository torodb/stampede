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
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class WriteBackendTransactionImpl extends BackendTransactionImpl implements WriteBackendTransaction {

    public WriteBackendTransactionImpl(SqlInterface sqlInterface, BackendConnectionImpl backendConnection,
            R2DTranslator<ResultSet> r2dTranslator) {
        super(sqlInterface.createWriteConnection(), sqlInterface, backendConnection, r2dTranslator);
    }
    
    @Override
    public void addDatabase(MetaDatabase db) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().addMetaDatabase(getDsl(), db.getName(), db.getIdentifier());
        getSqlInterface().createSchema(getDsl(), db.getIdentifier());
    }

    @Override
    public void addCollection(MetaDatabase db, MetaCollection newCol) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().addMetaCollection(getDsl(), db.getName(), newCol.getName(), newCol.getIdentifier());
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().addMetaDocPart(getDsl(), db.getName(), col.getName(),
                newDocPart.getTableRef(), newDocPart.getIdentifier());
    	
        Collection<? extends Field<?>> fields = getSqlInterface().getDocPartTableInternalFields(newDocPart);
        getSqlInterface().createDocPartTable(getDsl(), db.getIdentifier(), newDocPart.getIdentifier(), fields);
    }

    @Override
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField){
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().addMetaField(getDsl(), db.getName(), col.getName(), docPart.getTableRef(),
                newField.getName(), newField.getIdentifier(), newField.getType());
        getSqlInterface().addColumnToDocPartTable(getDsl(), db.getIdentifier(),
                docPart.getIdentifier(),buildField(newField));
    }

	@Override
	public void addScalar(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar newScalar) {
		Preconditions.checkState(!isClosed(), "This transaction is closed");

		getSqlInterface().addMetaScalar(getDsl(), db.getName(), col.getName(), docPart.getTableRef(),
				newScalar.getIdentifier(), newScalar.getType());
		getSqlInterface().addColumnToDocPartTable(getDsl(), db.getIdentifier(), docPart.getIdentifier(),
				buildScalar(newScalar));
	}
	
	private Field<?> buildField(MetaField newField) {
		return DSL.field(newField.getIdentifier(), getSqlInterface().getDataType(newField.getType()));
	}
	
	private Field<?> buildScalar(MetaScalar newScalar) {
		return DSL.field(newScalar.getIdentifier(), getSqlInterface().getDataType(newScalar.getType()));
	}

    @Override
    public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        return getSqlInterface().consumeRids(getDsl(), db.getName(), col.getName(), docPart.getTableRef(), howMany);
    }

    @Override
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");
        
        getSqlInterface().insertDocPartData(getDsl(), db.getIdentifier(), data);
    }

    @Override
    public void commit() throws UserException, RollbackException {
        Preconditions.checkState(!isClosed(), "This transaction is closed");
        
        try {
            getConnection().commit();
        } catch (SQLException ex) {
            getSqlInterface().handleUserAndRetryException(Context.commit, ex);
        } finally {
            getDsl().configuration().connectionProvider().release(getConnection());
        }
    }

}
