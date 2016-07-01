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
import com.torodb.core.backend.DidCursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
@Singleton
public abstract class AbstractReadInterface implements ReadInterface {
    
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
    public DidCursor getCollectionDidsWithFieldEqualsTo(DSLContext dsl, MetaDatabase metaDatabase,
            MetaCollection metaCol, MetaDocPart metaDocPart, MetaField metaField, KVValue<?> value)
            throws SQLException {
        assert metaDatabase.getMetaCollectionByIdentifier(metaCol.getIdentifier()) != null;
        assert metaCol.getMetaDocPartByIdentifier(metaDocPart.getIdentifier()) != null;
        assert metaDocPart.getMetaFieldByIdentifier(metaField.getIdentifier()) != null;

        String statement = getReadCollectionDidsWithFieldEqualsToStatement(metaDatabase.getIdentifier(), metaDocPart.getIdentifier(), metaField.getIdentifier());
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
    public DidCursor getAllCollectionDids(DSLContext dsl, MetaDatabase metaDatabase, MetaCollection metaCollection)
            throws SQLException {

        MetaDocPart rootDocPart = metaCollection.getMetaDocPartByTableRef(tableRefFactory.createRoot());
        if (rootDocPart == null) {
            return EmptyDidCursor.INSTANCE;
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

    @Nonnull
    @Override
    public DocPartResultBatch getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull DidCursor didCursor, int maxSize) throws SQLException {
        Collection<Integer> dids = didCursor.getNextBatch(maxSize);

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
	public int getLastRowIdUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart) {
		
		String statement = getLastRowIdUsedStatement(metaDatabase, metaDocPart);
		
		Connection connection = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement preparedStatement = connection.prepareStatement(statement)){
        	ResultSet rs = preparedStatement.executeQuery();
        	rs.next();
        	int maxId = rs.getInt(1);
        	if (rs.wasNull()){
        		return -1;
        	}
        	return maxId;
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
