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
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.*;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

public class WriteBackendTransactionImpl extends BackendTransactionImpl implements WriteBackendTransaction {

    private final IdentifierFactory identifierFactory;
    private final RidGenerator ridGenerator;
    
    public WriteBackendTransactionImpl(SqlInterface sqlInterface, BackendConnectionImpl backendConnection,
            R2DTranslator r2dTranslator, IdentifierFactory identifierFactory, RidGenerator ridGenerator) {
        super(sqlInterface.getDbBackend().createWriteConnection(), sqlInterface, backendConnection, r2dTranslator);
        
        this.identifierFactory = identifierFactory;
        this.ridGenerator = ridGenerator;
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
    public void renameCollection(MetaDatabase fromDb, MetaCollection fromColl, MutableMetaDatabase toDb, MutableMetaCollection toColl) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        copyMetaCollection(fromDb, fromColl, toDb, toColl);
        getSqlInterface().getStructureInterface().renameCollection(getDsl(), fromDb.getIdentifier(), fromColl,
                toDb.getIdentifier(), toColl);
        dropMetaCollection(fromDb.getName(), fromColl);
    }

    private void copyMetaCollection(MetaDatabase fromDb, MetaCollection fromColl,
            MutableMetaDatabase toDb, MutableMetaCollection toColl) {
        Iterator<? extends MetaDocPart> fromMetaDocPartIterator = fromColl.streamContainedMetaDocParts().iterator();
        while (fromMetaDocPartIterator.hasNext()) {
            MetaDocPart fromMetaDocPart = fromMetaDocPartIterator.next();
            MutableMetaDocPart toMetaDocPart = toColl.addMetaDocPart(fromMetaDocPart.getTableRef(), 
                    identifierFactory.toDocPartIdentifier(
                            toDb, toColl.getName(), fromMetaDocPart.getTableRef()));
            getSqlInterface().getMetaDataWriteInterface().addMetaDocPart(getDsl(), toDb.getName(), toColl.getName(),
                    toMetaDocPart.getTableRef(), toMetaDocPart.getIdentifier());
            copyScalar(identifierFactory, fromMetaDocPart, toDb, toColl, toMetaDocPart);
            copyFields(identifierFactory, fromMetaDocPart, toDb, toColl, toMetaDocPart);
            int nextRid = ridGenerator.getDocPartRidGenerator(fromDb.getName(), fromColl.getName()).nextRid(fromMetaDocPart.getTableRef());
            ridGenerator.getDocPartRidGenerator(toDb.getName(), toColl.getName()).setNextRid(toMetaDocPart.getTableRef(), nextRid - 1);
        }
    }

    private void copyScalar(IdentifierFactory identifierFactory, MetaDocPart fromMetaDocPart,
            MetaDatabase toMetaDb, MetaCollection toMetaColl, MutableMetaDocPart toMetaDocPart) {
        Iterator<? extends MetaScalar> fromMetaScalarIterator = fromMetaDocPart.streamScalars().iterator();
        while (fromMetaScalarIterator.hasNext()) {
            MetaScalar fromMetaScalar = fromMetaScalarIterator.next();
            MetaScalar toMetaScalar = toMetaDocPart.addMetaScalar(
                    identifierFactory.toFieldIdentifierForScalar(fromMetaScalar.getType()), 
                    fromMetaScalar.getType());
            getSqlInterface().getMetaDataWriteInterface().addMetaScalar(
                    getDsl(), toMetaDb.getName(), toMetaColl.getName(), toMetaDocPart.getTableRef(), 
                    toMetaScalar.getIdentifier(), toMetaScalar.getType());
        }
    }

    private void copyFields(IdentifierFactory identifierFactory, MetaDocPart fromMetaDocPart,
            MetaDatabase toMetaDb, MetaCollection toMetaColl, MutableMetaDocPart toMetaDocPart) {
        Iterator<? extends MetaField> fromMetaFieldIterator = fromMetaDocPart.streamFields().iterator();
        while (fromMetaFieldIterator.hasNext()) {
            MetaField fromMetaField = fromMetaFieldIterator.next();
            MetaField toMetaField = toMetaDocPart.addMetaField(
                    fromMetaField.getName(), 
                    identifierFactory.toFieldIdentifier(toMetaDocPart, fromMetaField.getType(), fromMetaField.getName()), 
                    fromMetaField.getType());
            getSqlInterface().getMetaDataWriteInterface().addMetaField(
                    getDsl(), toMetaDb.getName(), toMetaColl.getName(), toMetaDocPart.getTableRef(), 
                    toMetaField.getName(), toMetaField.getIdentifier(), toMetaField.getType());
        }
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
            getSqlInterface().getStructureInterface().createIndex(getDsl(), db.getIdentifier(), docPart.getIdentifier(), newField.getIdentifier(), true);
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
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) {
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
