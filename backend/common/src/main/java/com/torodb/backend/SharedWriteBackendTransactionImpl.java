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

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.TableRef;
import com.torodb.core.backend.SharedWriteBackendTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;

public class SharedWriteBackendTransactionImpl extends BackendTransactionImpl implements SharedWriteBackendTransaction {

    public SharedWriteBackendTransactionImpl(SqlInterface sqlInterface, BackendConnectionImpl backendConnection,
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
    public void dropCollection(MetaDatabase db, MetaCollection coll) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        dropMetaCollection(db.getName(), coll);
        getSqlInterface().getStructureInterface().dropCollection(getDsl(), db.getIdentifier(), coll);
    }

    @Override
    public void dropDatabase(MetaDatabase db) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        Iterator<? extends MetaCollection> metaCollectionIterator = db.streamMetaCollections().iterator();
        while (metaCollectionIterator.hasNext()) {
            MetaCollection metaCollection = metaCollectionIterator.next();
            dropMetaCollection(db.getName(), metaCollection);
        }
        getSqlInterface().getMetaDataWriteInterface().deleteMetaDatabase(getDsl(), db.getName());
        getSqlInterface().getStructureInterface().dropDatabase(getDsl(), db);
    }

    protected void dropMetaCollection(String databaseName, MetaCollection coll) {
        Iterator<? extends MetaDocPart> metaDocPartIterator = coll.streamContainedMetaDocParts().iterator();
        while (metaDocPartIterator.hasNext()) {
            MetaDocPart metaDocPart = metaDocPartIterator.next();
            dropMetaDocPart(databaseName, coll, metaDocPart);
        }
        getSqlInterface().getMetaDataWriteInterface().deleteMetaCollection(getDsl(), databaseName, coll.getName());
    }

    private void dropMetaDocPart(String databaseName, MetaCollection coll, MetaDocPart metaDocPart) {
        dropMetaScalars(databaseName, coll, metaDocPart);
        dropMetaFields(databaseName, coll, metaDocPart);
        getSqlInterface().getMetaDataWriteInterface().deleteMetaDocPart(getDsl(), databaseName, coll.getName(), 
                metaDocPart.getTableRef());
    }

    private void dropMetaScalars(String databaseName, MetaCollection coll, MetaDocPart metaDocPart) {
        Iterator<? extends MetaScalar> metaScalarIterator = metaDocPart.streamScalars().iterator();
        while (metaScalarIterator.hasNext()) {
            MetaScalar metaScalar = metaScalarIterator.next();
            getSqlInterface().getMetaDataWriteInterface().deleteMetaScalar(getDsl(), databaseName, coll.getName(), 
                    metaDocPart.getTableRef(), metaScalar.getType());
        }
    }

    private void dropMetaFields(String databaseName, MetaCollection coll, MetaDocPart metaDocPart) {
        Iterator<? extends MetaField> metaFieldIterator = metaDocPart.streamFields().iterator();
        while (metaFieldIterator.hasNext()) {
            MetaField metaField = metaFieldIterator.next();
            getSqlInterface().getMetaDataWriteInterface().deleteMetaField(getDsl(), databaseName, coll.getName(), 
                    metaDocPart.getTableRef(), metaField.getName(), metaField.getType());
        }
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaDocPart(getDsl(), db.getName(), col.getName(),
                newDocPart.getTableRef(), newDocPart.getIdentifier());
    	
        TableRef tableRef = newDocPart.getTableRef();
    	if (tableRef.isRoot()) {
            getSqlInterface().getStructureInterface().createRootDocPartTable(getDsl(), db.getIdentifier(), newDocPart.getIdentifier(), tableRef);
            getSqlInterface().getStructureInterface().streamRootDocPartTableIndexesCreation(db.getIdentifier(), newDocPart.getIdentifier(), tableRef)
                    .forEach(consumer -> consumer.accept(getDsl()));
    	} else {
            getSqlInterface().getStructureInterface().createDocPartTable(getDsl(), db.getIdentifier(), newDocPart.getIdentifier(), tableRef,
                    col.getMetaDocPartByTableRef(tableRef.getParent().get()).getIdentifier());
            getSqlInterface().getStructureInterface().streamDocPartTableIndexesCreation(db.getIdentifier(), newDocPart.getIdentifier(), tableRef,
                    col.getMetaDocPartByTableRef(tableRef.getParent().get()).getIdentifier())
                    .forEach(consumer -> consumer.accept(getDsl()));;
    	}
    }

    @Override
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField){
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaField(getDsl(), db.getName(), col.getName(), docPart.getTableRef(),
                newField.getName(), newField.getIdentifier(), newField.getType());
        getSqlInterface().getStructureInterface().addColumnToDocPartTable(getDsl(), db.getIdentifier(),
                docPart.getIdentifier(), newField.getIdentifier(), getSqlInterface().getDataTypeProvider().getDataType(newField.getType()));
        
        //TODO: This is a hack accepted by all devs. Mongolization for create an index on _id fields
        if (docPart.getTableRef().isRoot() && "_id".equals(newField.getName())) {
            getSqlInterface().getStructureInterface().createIndex(getDsl(), db.getIdentifier(), docPart.getIdentifier(), newField.getIdentifier(), true, true);
        }
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
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) throws UserException {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        getSqlInterface().getWriteInterface().insertDocPartData(getDsl(), db.getIdentifier(), data);
    }

    @Override
    public void deleteDids(MetaDatabase db, MetaCollection col, Collection<Integer> dids) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        if (dids.isEmpty()) {
            return ;
        }

        getSqlInterface().getWriteInterface().deleteCollectionDocParts(getDsl(), db.getIdentifier(), col, dids);
    }

    @Override
    public void commit() throws UserException, RollbackException {
        Preconditions.checkState(!isClosed(), "This transaction is closed");
        
        try {
            getConnection().commit();
        } catch (SQLException ex) {
            getSqlInterface().getErrorHandler().handleUserException(Context.COMMIT, ex);
        } finally {
            getDsl().configuration().connectionProvider().release(getConnection());
        }
    }
}
