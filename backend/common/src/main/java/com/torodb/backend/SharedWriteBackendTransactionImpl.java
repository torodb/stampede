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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.TableRef;
import com.torodb.core.backend.SharedWriteBackendTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;

public class SharedWriteBackendTransactionImpl extends BackendTransactionImpl implements SharedWriteBackendTransaction {

    private final IdentifierFactory identifierFactory;

    public SharedWriteBackendTransactionImpl(SqlInterface sqlInterface, BackendConnectionImpl backendConnection,
            R2DTranslator r2dTranslator, IdentifierFactory identifierFactory) {
        super(sqlInterface.getDbBackend().createWriteConnection(), sqlInterface, backendConnection, r2dTranslator);
        
        this.identifierFactory = identifierFactory;
    }
    
    @Override
    public void addDatabase(MetaDatabase db) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaDatabase(getDsl(), db);
        getSqlInterface().getStructureInterface().createSchema(getDsl(), db.getIdentifier());
    }

    @Override
    public void addCollection(MetaDatabase db, MetaCollection newCol) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaCollection(getDsl(), db, newCol);
    }

    @Override
    public void dropCollection(MetaDatabase db, MetaCollection coll) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        dropMetaCollection(db, coll);
        getSqlInterface().getStructureInterface().dropCollection(getDsl(), db.getIdentifier(), coll);
    }

    @Override
    public void dropDatabase(MetaDatabase db) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        Iterator<? extends MetaCollection> metaCollectionIterator = db.streamMetaCollections().iterator();
        while (metaCollectionIterator.hasNext()) {
            MetaCollection metaCollection = metaCollectionIterator.next();
            dropMetaCollection(db, metaCollection);
        }
        getSqlInterface().getMetaDataWriteInterface().deleteMetaDatabase(getDsl(), db);
        getSqlInterface().getStructureInterface().dropDatabase(getDsl(), db);
    }

    protected void dropMetaCollection(MetaDatabase database, MetaCollection coll) {
        getSqlInterface().getMetaDataWriteInterface().deleteMetaCollection(getDsl(), database, coll);
    }

    @Override
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaDocPart(getDsl(), db, col,
                newDocPart);
    	
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
    public void addField(MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart, MetaField newField){
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaField(getDsl(), db, col, docPart,
                newField);
        getSqlInterface().getStructureInterface().addColumnToDocPartTable(getDsl(), db.getIdentifier(),
                docPart.getIdentifier(), newField.getIdentifier(), getSqlInterface().getDataTypeProvider().getDataType(newField.getType()));
        
        col.streamContainedMetaIndexes()
            .filter(index -> index.getMetaIndexFieldByTableRefAndName(docPart.getTableRef(), newField.getName()) != null)
            .forEach(index -> { 
                if (index.size() > 1) {
                    throw new UnsupportedOperationException("Index with more than one field is not supported");
                }
            });
        Optional<? extends MetaIndex> optionalCompatibleIndex = col.streamContainedMetaIndexes()
                .filter(index -> index.getMetaIndexFieldByTableRefAndName(docPart.getTableRef(), newField.getName()) != null)
                .findAny();
        
        if (optionalCompatibleIndex.isPresent()) {
            MetaIndex compatibleIndex = optionalCompatibleIndex.get();  
            MutableMetaDocPartIndex docPartIndex = docPart.addMetaDocPartIndex(identifierFactory.toIndexIdentifier( 
                    db, docPart.getIdentifier(), Arrays.asList(new String[] { newField.getIdentifier() })), 
                    compatibleIndex.isUnique());
            getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndex(getDsl(), db, col, docPart, docPartIndex);
            
            MetaIndexField indexField = compatibleIndex.iteratorFields().next();
            MetaDocPartIndexColumn fieldIndex = docPartIndex.addMetaDocPartIndexColumn(newField.getIdentifier(), indexField.getOrdering());
            getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndexColumn(getDsl(), db, col, docPart, docPartIndex, fieldIndex);
            
            getSqlInterface().getStructureInterface().createIndex(getDsl(), docPartIndex.getIdentifier(), db.getIdentifier(), 
                    docPart.getIdentifier(), newField.getIdentifier(), fieldIndex.getOrdering().isAscending(), docPartIndex.isUnique());
        }
    }

	@Override
	public void addScalar(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar newScalar) {
		Preconditions.checkState(!isClosed(), "This transaction is closed");

        getSqlInterface().getMetaDataWriteInterface().addMetaScalar(getDsl(), db, col, docPart,
				newScalar);
		getSqlInterface().getStructureInterface().addColumnToDocPartTable(getDsl(), db.getIdentifier(), docPart.getIdentifier(),
		        newScalar.getIdentifier(), getSqlInterface().getDataTypeProvider().getDataType(newScalar.getType()));
	}
    
    @Override
    public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        return getSqlInterface().getMetaDataWriteInterface().consumeRids(getDsl(), db, col, docPart, howMany);
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
    public void createIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index) {
        if (index.size() > 1) {
            throw new UnsupportedOperationException("Index with more than one field is not supported");
        }
        
        getSqlInterface().getMetaDataWriteInterface().addMetaIndex(getDsl(), db, col, index);
        
        Iterator<? extends MetaIndexField> indexFieldIterator = index.iteratorFields();
        while (indexFieldIterator.hasNext()) {
            MetaIndexField field = indexFieldIterator.next();
            getSqlInterface().getMetaDataWriteInterface().addMetaIndexField(getDsl(), db, col, index, field);
        }
        
        createMissingDocPartIndexes(db, col, index);
    }

    private void createMissingDocPartIndexes(MetaDatabase db, MutableMetaCollection col, MetaIndex index) {
        Iterator<TableRef> tableRefIterator = index.streamTableRefs().iterator();
        while (tableRefIterator.hasNext()) {
            TableRef tableRef = tableRefIterator.next();
            MutableMetaDocPart docPart = col.getMetaDocPartByTableRef(tableRef);
            if (docPart != null) {
                Iterator<List<String>> docPartIndexesFieldsIterator = 
                        index.streamMetaDocPartIndexesIdentifiers(docPart).iterator();
                
                while (docPartIndexesFieldsIterator.hasNext()) {
                    List<String> identifiers = docPartIndexesFieldsIterator.next();
                    boolean containsExactDocPartIndex = docPart.streamIndexes()
                            .anyMatch(docPartIndex -> index.isMatch(docPart, identifiers, docPartIndex));
                    if (!containsExactDocPartIndex) {
                        createOneFieldIndex(db, col, index, tableRef, docPart, identifiers);
                    }
                }
            }
        }
    }

    private void createOneFieldIndex(MetaDatabase db, MetaCollection col, MetaIndex index, TableRef tableRef, MutableMetaDocPart docPart,
            List<String> identifiers) {
        Iterator<? extends MetaIndexField> indexFieldIterator;
        MutableMetaDocPartIndex docPartIndex = docPart.addMetaDocPartIndex(
                identifierFactory.toIndexIdentifier(db, docPart.getIdentifier(), identifiers), index.isUnique());
        getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndex(getDsl(), db, col, docPart, docPartIndex);
        indexFieldIterator = index.iteratorMetaIndexFieldByTableRef(tableRef);
        for (String identifier : identifiers) {
            MetaIndexField indexField = indexFieldIterator.next();
            MetaDocPartIndexColumn docPartIndexColumn = docPartIndex.addMetaDocPartIndexColumn(identifier, indexField.getOrdering());
            getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndexColumn(getDsl(), db, col, docPart, docPartIndex, docPartIndexColumn);
        }
        
        getSqlInterface().getStructureInterface().createIndex(
                getDsl(), docPartIndex.getIdentifier(), db.getIdentifier(), docPart.getIdentifier(), 
                identifiers.get(0), index.iteratorMetaIndexFieldByTableRef(tableRef)
                    .next().getOrdering().isAscending(), index.isUnique());
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
