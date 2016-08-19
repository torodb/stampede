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

import com.google.common.collect.Multimap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

import org.jooq.DSLContext;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.d2r.ResultSetDocPartResult;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.EmptyCursor;
import com.torodb.core.cursors.IteratorCursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.transaction.metainf.*;
import com.torodb.kvdocument.values.KVValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.Unchecked;

/**
 *
 */
@Singleton
@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public abstract class AbstractReadInterface implements ReadInterface {

    private static final Logger LOGGER = LogManager.getLogger(AbstractReadInterface.class);
    private final MetaDataReadInterface metaDataReadInterface;
    private final DataTypeProvider dataTypeProvider;
    private final ErrorHandler errorHandler;
    private final SqlHelper sqlHelper;
    private final TableRefFactory tableRefFactory;

    public AbstractReadInterface(MetaDataReadInterface metaDataReadInterface, DataTypeProvider dataTypeProvider,
        ErrorHandler errorHandler, SqlHelper sqlHelper, TableRefFactory tableRefFactory) {
        this.metaDataReadInterface = metaDataReadInterface;
        this.dataTypeProvider = dataTypeProvider;
        this.errorHandler = errorHandler;
        this.sqlHelper = sqlHelper;
        this.tableRefFactory = tableRefFactory;
    }

    @Override
    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION","ODR_OPEN_DATABASE_RESOURCE"},
    justification = "ResultSet is wrapped in a Cursor<Integer>. It's iterated and closed in caller code")
    public Cursor<Integer> getCollectionDidsWithFieldEqualsTo(DSLContext dsl, MetaDatabase metaDatabase,
            MetaCollection metaCol, MetaDocPart metaDocPart, MetaField metaField, KVValue<?> value)
            throws SQLException {
        assert metaDatabase.getMetaCollectionByIdentifier(metaCol.getIdentifier()) != null;
        assert metaCol.getMetaDocPartByIdentifier(metaDocPart.getIdentifier()) != null;
        assert metaDocPart.getMetaFieldByIdentifier(metaField.getIdentifier()) != null;

		String statement = getReadCollectionDidsWithFieldEqualsToStatement(metaDatabase.getIdentifier(),
				metaDocPart.getIdentifier(), metaField.getIdentifier());
        Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(statement);
            sqlHelper.setPreparedStatementValue(preparedStatement, 1, metaField.getType(), value);
            return new DefaultDidCursor(errorHandler, preparedStatement.executeQuery());
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }
    
    protected abstract String getReadCollectionDidsWithFieldEqualsToStatement(String schemaName, String rootTableName,
            String columnName);

    @Override
    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION","ODR_OPEN_DATABASE_RESOURCE"},
    justification = "ResultSet is wrapped in a Cursor<Integer>. It's iterated and closed in caller code")
    @SuppressWarnings("unchecked")
    public Cursor<Integer> getCollectionDidsWithFieldsIn(DSLContext dsl, MetaDatabase metaDatabase,
            MetaCollection metaCol, MetaDocPart metaDocPart, Multimap<MetaField, KVValue<?>> valuesMultimap)
            throws SQLException {
        assert metaDatabase.getMetaCollectionByIdentifier(metaCol.getIdentifier()) != null;
        assert metaCol.getMetaDocPartByIdentifier(metaDocPart.getIdentifier()) != null;
        assert valuesMultimap.keySet().stream().allMatch(metafield -> metaDocPart.getMetaFieldByIdentifier(metafield.getIdentifier()) != null);

        LOGGER.warn("A very inefficient implementation of getCollectionDidsWithFieldsIn is being used");

        Stream<Integer> didStream = valuesMultimap.entries().stream()
                .map(Unchecked.function(
                        (Map.Entry<MetaField, KVValue<?>> entry)
                        -> getCollectionDidsWithFieldEqualsTo(
                                dsl,
                                metaDatabase,
                                metaCol,
                                metaDocPart,
                                entry.getKey(),
                                entry.getValue())
                ))
                .flatMap((Cursor<Integer> cursor) -> cursor.getRemaining().stream());

        return new IteratorCursor<>(didStream.iterator());
    }

    @Override
    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION","ODR_OPEN_DATABASE_RESOURCE"},
    justification = "ResultSet is wrapped in a Cursor<Integer>. It's iterated and closed in caller code")
    public Cursor<Integer> getAllCollectionDids(DSLContext dsl, MetaDatabase metaDatabase, MetaCollection metaCollection)
            throws SQLException {

        MetaDocPart rootDocPart = metaCollection.getMetaDocPartByTableRef(tableRefFactory.createRoot());
        if (rootDocPart == null) {
            return new EmptyCursor<>();
        }

        String statement = getReadAllCollectionDidsStatement(metaDatabase.getIdentifier(), rootDocPart.getIdentifier());
        Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(statement);
            return new DefaultDidCursor(errorHandler, preparedStatement.executeQuery());
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    protected abstract String getReadAllCollectionDidsStatement(String schemaName, String rootTableName);

    @Override
    public long countAll(
            @Nonnull DSLContext dsl,
            @Nonnull MetaDatabase database,
            @Nonnull MetaCollection collection
            ) {
        MetaDocPart rootDocPart = collection.getMetaDocPartByTableRef(tableRefFactory.createRoot());
        if (rootDocPart == null) {
            return 0;
        }
        String statement = getReadCountAllStatement(database.getIdentifier(), rootDocPart.getIdentifier());
        return sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH)
                .get(0).into(Long.class);
    }

    protected abstract String getReadCountAllStatement(String schema, String rootTableName);

    @Nonnull
    @Override
    public DocPartResultBatch getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull Cursor<Integer> didCursor, int maxSize) throws SQLException {
        Collection<Integer> dids = didCursor.getNextBatch(maxSize);
        return getCollectionResultSets(dsl, metaDatabase, metaCollection, dids);
    }

    @Override
    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION","ODR_OPEN_DATABASE_RESOURCE"},
    justification = "ResultSet is wrapped in a ResultSetDocPartResult. It's iterated and closed in caller code")
    public DocPartResultBatch getCollectionResultSets(DSLContext dsl, MetaDatabase metaDatabase,
            MetaCollection metaCollection, Collection<Integer> dids) throws SQLException {
        ArrayList<DocPartResult> result = new ArrayList<>();
        Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
            Iterator<? extends MetaDocPart> metaDocPartIterator = metaCollection
                    .streamContainedMetaDocParts()
                    .sorted(TableRefComparator.MetaDocPart.DESC)
                    .iterator();
            while (metaDocPartIterator.hasNext()) {
                MetaDocPart metaDocPart = metaDocPartIterator.next();
                String statament = getDocPartStatament(metaDatabase, metaDocPart, dids);
    
                PreparedStatement preparedStatement = connection.prepareStatement(statament);
                result.add(new ResultSetDocPartResult(metaDataReadInterface, dataTypeProvider, errorHandler, 
                        metaDocPart, preparedStatement.executeQuery(), sqlHelper));
            }
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
        return new DocPartResultBatch(result);
    }

    protected abstract String getDocPartStatament(MetaDatabase metaDatabase, MetaDocPart metaDocPart,
            Collection<Integer> dids);

	@Override
	public int getLastRowIdUsed(DSLContext dsl, MetaDatabase metaDatabase, MetaCollection metaCollection, MetaDocPart metaDocPart) {
		
		String statement = getLastRowIdUsedStatement(metaDatabase, metaDocPart);
		
		Connection connection = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement preparedStatement = connection.prepareStatement(statement)){
        	try (ResultSet rs = preparedStatement.executeQuery()){
	        	rs.next();
	        	int maxId = rs.getInt(1);
	        	if (rs.wasNull()){
	        		return -1;
	        	}
	        	return maxId;
        	}
        } catch (SQLException ex){
            throw errorHandler.handleException(Context.FETCH, ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
	}

    protected abstract String getLastRowIdUsedStatement(MetaDatabase metaDatabase, MetaDocPart metaDocPart);
	
	protected String getPrimaryKeyColumnIdentifier(TableRef tableRef){
		 if (tableRef.isRoot()){
			 return DocPartTableFields.DID.fieldName;
         }
		 return DocPartTableFields.RID.fieldName;
	}
}
