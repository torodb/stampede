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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

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
import com.torodb.core.transaction.metainf.MetaDocPartIndex;
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
    public void addField(MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart, MetaField newField) throws UserException {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

    	getSqlInterface().getMetaDataWriteInterface().addMetaField(getDsl(), db, col, docPart,
                newField);
        getSqlInterface().getStructureInterface().addColumnToDocPartTable(getDsl(), db.getIdentifier(),
                docPart.getIdentifier(), newField.getIdentifier(), getSqlInterface().getDataTypeProvider().getDataType(newField.getType()));

        List<Tuple2<MetaIndex, List<String>>> missingIndexesParts = col.streamContainedMetaIndexes()
            .filter(index -> index.getMetaIndexFieldByTableRefAndName(docPart.getTableRef(), newField.getName()) != null)
            .flatMap(index -> Seq.seq(index.iteratorMetaDocPartIndexesIdentifiers(docPart))
                    .filter(identifiers -> identifiers.contains(newField.getIdentifier()))
                    .map(identifiers -> new Tuple2<MetaIndex,List<String>>(index, identifiers)))
            .collect(Collectors.toList());
        
        for (Tuple2<MetaIndex, List<String>> missingIndexEntry : missingIndexesParts) {
            MetaIndex missingIndex = missingIndexEntry.v1();
            List<String> identifiers = missingIndexEntry.v2();
            int position = identifiers.indexOf(newField.getIdentifier());
            Optional<MutableMetaDocPartIndex> matchingMutableDocPartIndex = Seq.seq(docPart.getAddedMutableMetaDocPartIndexes())
                    .filter(docPartIndex -> docPartIndex.getMetaDocPartIndexColumnByPosition(position) == null && 
                        missingIndex.isSubMatch(docPart, identifiers, docPartIndex) &&
                        // We ensure we do not pick a doc part index that fit a isSubMatch for our index but
                        // was the only chance for another combination. For example:
                        // 1. a_i, b_i, c_i are old fields
                        // 2. a_s, b_s, c_s are new fields
                        // 3. we have index a asc, b asc, c asc
                        // 4. we added doc part index a_s asc, null, null and a_s asc, b_i asc, null
                        // 5. we search for a sub match for a_s, b_i, c_s and found a_s asc, null, null
                        noneNonCurrentAndNullIndexColumnIsNew(position, docPartIndex, docPart, identifiers))
                    .findAny();
            MutableMetaDocPartIndex docPartIndex;
            if (matchingMutableDocPartIndex.isPresent()) {
                docPartIndex = matchingMutableDocPartIndex.get();
            } else {
                docPartIndex = docPart.addMetaDocPartIndex(missingIndex.isUnique());
                int index = 0;
                for (String identifier : identifiers) {
                    if (docPart.getAddedFieldByIdentifier(identifier) == null) {
                        MetaIndexField indexField = missingIndex.getMetaIndexFieldByTableRefAndPosition(docPart.getTableRef(), index);
                        docPartIndex.putMetaDocPartIndexColumn(index, identifier, indexField.getOrdering());
                    }
                    index++;
                }
            }
            MetaIndexField indexField = missingIndex.getMetaIndexFieldByTableRefAndPosition(docPart.getTableRef(), position);
            docPartIndex.putMetaDocPartIndexColumn(position, newField.getIdentifier(), indexField.getOrdering());
            if (missingIndex.isMatch(docPart, identifiers, docPartIndex)) {
                List<Tuple2<String, Boolean>> columnList = new ArrayList<>(docPartIndex.size());
                for (String identifier : identifiers) {
                    MetaDocPartIndexColumn docPartIndexColumn = docPartIndex.getMetaDocPartIndexColumnByIdentifier(identifier);
                    columnList.add(new Tuple2<>(docPartIndexColumn.getIdentifier(), docPartIndexColumn.getOrdering().isAscending()));
                }
                docPartIndex.makeImmutable(identifierFactory.toIndexIdentifier(db, docPart.getIdentifier(), columnList));

                getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndex(getDsl(), db, col, docPart, docPartIndex);

                for (String identifier : identifiers) {
                    MetaDocPartIndexColumn docPartIndexColumn = docPartIndex.getMetaDocPartIndexColumnByIdentifier(identifier);
                    getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndexColumn(getDsl(), db, col, docPart, docPartIndex, docPartIndexColumn);
                }
                
                getSqlInterface().getStructureInterface().createIndex(getDsl(), docPartIndex.getIdentifier(), db.getIdentifier(), 
                        docPart.getIdentifier(), columnList, docPartIndex.isUnique());
            }
        }
    }

    private boolean noneNonCurrentAndNullIndexColumnIsNew(int position, MutableMetaDocPartIndex docPartIndex,
            MutableMetaDocPart docPart, List<String> identifiers) {
        return IntStream.range(0, identifiers.size())
            .noneMatch(index -> index != position && 
                docPartIndex.getMetaDocPartIndexColumnByPosition(index) == null &&
                docPart.getAddedFieldByIdentifier(identifiers.get(index)) == null);
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
    public void createIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index) throws UserException {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        Preconditions.checkArgument(!index.isUnique() || index.streamTableRefs().count() == 1, 
                "composed unique indexes on fields of different subdocuments are not supported yet");
        
        getSqlInterface().getMetaDataWriteInterface().addMetaIndex(getDsl(), db, col, index);
        
        Iterator<? extends MetaIndexField> indexFieldIterator = index.iteratorFields();
        while (indexFieldIterator.hasNext()) {
            MetaIndexField field = indexFieldIterator.next();
            getSqlInterface().getMetaDataWriteInterface().addMetaIndexField(getDsl(), db, col, index, field);
        }
        
        createMissingDocPartIndexes(db, col, index);
    }

    private void createMissingDocPartIndexes(MetaDatabase db, MutableMetaCollection col, MetaIndex index) throws UserException {
        Iterator<TableRef> tableRefIterator = index.streamTableRefs().iterator();
        while (tableRefIterator.hasNext()) {
            TableRef tableRef = tableRefIterator.next();
            MutableMetaDocPart docPart = col.getMetaDocPartByTableRef(tableRef);
            if (docPart != null && index.isCompatible(docPart)) {
                Iterator<List<String>> docPartIndexesFieldsIterator = 
                        index.iteratorMetaDocPartIndexesIdentifiers(docPart);
                
                while (docPartIndexesFieldsIterator.hasNext()) {
                    List<String> identifiers = docPartIndexesFieldsIterator.next();
                    boolean containsExactDocPartIndex = docPart.streamIndexes()
                            .anyMatch(docPartIndex -> index.isMatch(docPart, identifiers, docPartIndex));
                    if (!containsExactDocPartIndex) {
                        createIndex(db, col, index, docPart, identifiers);
                    }
                }
            }
        }
    }

    private void createIndex(MetaDatabase db, MetaCollection col, MetaIndex index, MutableMetaDocPart docPart,
            List<String> identifiers) throws UserException {
        MutableMetaDocPartIndex docPartIndex = docPart.addMetaDocPartIndex(index.isUnique());
        Iterator<? extends MetaIndexField> indexFieldIterator = index.iteratorMetaIndexFieldByTableRef(docPart.getTableRef());
        int position = 0;
        List<Tuple2<String, Boolean>> columnList = new ArrayList<>(identifiers.size());
        for (String identifier : identifiers) {
            MetaIndexField indexField = indexFieldIterator.next();
            MetaDocPartIndexColumn docPartIndexColumn = docPartIndex.putMetaDocPartIndexColumn(position++, identifier, indexField.getOrdering());
            columnList.add(new Tuple2<>(docPartIndexColumn.getIdentifier(), docPartIndexColumn.getOrdering().isAscending()));
        }
        docPartIndex.makeImmutable(identifierFactory.toIndexIdentifier(db, docPart.getIdentifier(), columnList));

        getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndex(getDsl(), db, col, docPart, docPartIndex);
        
        for (String identifier : identifiers) {
            MetaDocPartIndexColumn docPartIndexColumn = docPartIndex.getMetaDocPartIndexColumnByIdentifier(identifier);
            getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndexColumn(getDsl(), db, col, docPart, docPartIndex, docPartIndexColumn);
        }
        
        getSqlInterface().getStructureInterface().createIndex(
                getDsl(), docPartIndex.getIdentifier(), db.getIdentifier(), docPart.getIdentifier(), 
                columnList, index.isUnique());
    }

    @Override
    public void dropIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        getSqlInterface().getMetaDataWriteInterface().deleteMetaIndex(getDsl(), db, col, index);
        Iterator<TableRef> tableRefIterator = index.streamTableRefs().iterator();
        while (tableRefIterator.hasNext()) {
            TableRef tableRef = tableRefIterator.next();
            MutableMetaDocPart docPart = col.getMetaDocPartByTableRef(tableRef);
            if (docPart != null) {
                Iterator<? extends MetaDocPartIndex> docPartIndexesIterator = 
                        docPart.streamIndexes().iterator();
                
                while (docPartIndexesIterator.hasNext()) {
                    MetaDocPartIndex docPartIndex = docPartIndexesIterator.next();
                    if (index.isCompatible(docPart, docPartIndex)) {
                        boolean existsAnyOtherCompatibleIndex = col.streamContainedMetaIndexes()
                                .anyMatch(otherIndex -> otherIndex != index &&
                                    otherIndex.isCompatible(docPart, docPartIndex));
                        if (!existsAnyOtherCompatibleIndex) {
                            dropIndex(db, col, docPart, docPartIndex);
                        }
                    }
                }
            }
        }
    }

    private void dropIndex(MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart,
            MetaDocPartIndex docPartIndex) {
        docPart.removeMetaDocPartIndexByIdentifier(docPartIndex.getIdentifier());
        
        getSqlInterface().getMetaDataWriteInterface().deleteMetaDocPartIndex(getDsl(), db, col, docPart, docPartIndex);
        
        getSqlInterface().getStructureInterface().dropIndex(
                getDsl(), db.getIdentifier(), docPartIndex.getIdentifier());
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
