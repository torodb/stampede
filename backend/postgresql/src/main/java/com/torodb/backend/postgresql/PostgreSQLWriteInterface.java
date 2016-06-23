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

package com.torodb.backend.postgresql;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import com.torodb.backend.AbstractWriteInterface;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.postgresql.converters.PostgreSQLValueToCopyConverter;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
@Singleton
public class PostgreSQLWriteInterface extends AbstractWriteInterface {
    
    private static final Logger LOGGER = LogManager.getLogger(PostgreSQLWriteInterface.class);
    
    private final PostgreSQLMetaDataReadInterface postgreSQLMetaDataReadInterface;
    private final ErrorHandler errorHandler;
    
    @Inject
    public PostgreSQLWriteInterface(PostgreSQLMetaDataReadInterface postgreSQLMetaDataReadInterface,
            PostgreSQLDataTypeProvider postgreSQLDataTypeProvider, ErrorHandler errorHandler,
            SqlHelper sqlHelper) {
        super(postgreSQLMetaDataReadInterface, postgreSQLDataTypeProvider, errorHandler, sqlHelper);
        this.postgreSQLMetaDataReadInterface = postgreSQLMetaDataReadInterface;
        this.errorHandler = errorHandler;
    }

    @Override
    protected String getDeleteDocPartsStatement(String schemaName, String tableName, List<Integer> dids) {
        StringBuilder sb = new StringBuilder()
                .append("DELETE FROM \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" WHERE \"")
                .append(MetaDocPartTable.DocPartTableFields.DID.fieldName)
                .append("\" IN (");
        for (Integer did : dids) {
            sb.append(did)
                .append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        String statement = sb.toString();
        return statement;
    }
    
    @Override
    public void insertDocPartData(DSLContext dsl, String schemaName, DocPartData docPartData) {
        if (docPartData.rowCount()==0){
            return;
        }
        int maxCappedSize = 10;
        int cappedSize = Math.min(docPartData.rowCount(), maxCappedSize);

        if (cappedSize < maxCappedSize) { //there are not enough elements on the insert => fallback
            LOGGER.debug(
                    "The insert window is not big enough to use copy (the "
                            + "limit is {}, the real size is {}).",
                    maxCappedSize,
                    cappedSize
            );
            super.insertDocPartData(dsl, schemaName, docPartData);
        }
        else {
            Connection connection = dsl.configuration().connectionProvider().acquire();
            try {
                if (!connection.isWrapperFor(PGConnection.class)) {
                    LOGGER.warn("It was impossible to use the PostgreSQL way to "
                            + "insert documents. Inserting using the standard "
                            + "implementation");
                    super.insertDocPartData(dsl, schemaName, docPartData);
                } else {
                    try {
                        copyInsertDocPartData(
                                connection.unwrap(PGConnection.class),
                                schemaName,
                                docPartData
                                );
                    } catch (DataAccessException ex) {
                        errorHandler.handleRollbackException(Context.insert, ex);
                        throw new SystemException(ex);
                    } catch (SQLException ex) {
                        errorHandler.handleRollbackException(Context.insert, ex);
                        throw new SystemException(ex);
                    } catch (IOException ex) {
                        throw new SystemException(ex);
                    }
                }
            } catch (SQLException ex) {
                throw new SystemException(ex);
            } finally {
                dsl.configuration().connectionProvider().release(connection);
            }
        }
    }
    
    private void copyInsertDocPartData(
            PGConnection connection,
            String schemaName,
            DocPartData docPartData) throws SQLException, IOException {

        final int maxBatchSize = 1000;
        final CopyManager copyManager = connection.getCopyAPI();
        final MetaDocPart metaDocPart = docPartData.getMetaDocPart();
        Collection<InternalField<?>> internalFields = postgreSQLMetaDataReadInterface
                .getDocPartTableInternalFields(metaDocPart);
        final StringBuilder sb = new StringBuilder(2048);
        final String copyStatement = getCopyInsertDocPartDataStatement(
                schemaName, docPartData, metaDocPart, internalFields);
        
        Iterator<DocPartRow> docPartRowIterator = docPartData.iterator();
        int docCounter = 0;
        while (docPartRowIterator.hasNext()) {
            DocPartRow tableRow = docPartRowIterator.next();
            docCounter++;

            addValuesToCopy(sb, tableRow, internalFields);
            assert sb.length() != 0;

            if (docCounter % maxBatchSize == 0 || !docPartRowIterator.hasNext()) {
                executeCopy(copyManager, copyStatement, sb);
                assert sb.length() == 0;
            }
        }
        if (sb.length() > 0) {
            assert docCounter % maxBatchSize != 0;
            executeCopy(copyManager, copyStatement, sb);
            
            if (docPartRowIterator.hasNext()) {
                sb.delete(0, sb.length());
            }
        }
    }

    protected String getCopyInsertDocPartDataStatement(String schemaName, DocPartData docPartData,
            final MetaDocPart metaDocPart, Collection<InternalField<?>> internalFields) {
        final StringBuilder copyStatementBuilder = new StringBuilder();
        copyStatementBuilder.append("COPY \"")
            .append(schemaName)
            .append("\".\"").append(metaDocPart.getIdentifier())
            .append("\"").append(" (");
        
        for (InternalField<?> internalField : internalFields) {
            copyStatementBuilder.append("\"")
                .append(internalField.getName())
                .append("\",");
        }
        Iterator<MetaScalar> metaScalarIterator = docPartData.orderedMetaScalarIterator();
        while (metaScalarIterator.hasNext()) {
            MetaScalar metaScalar = metaScalarIterator.next();
            copyStatementBuilder.append("\"")
                .append(metaScalar.getIdentifier())
                .append("\",");
        }
        Iterator<MetaField> metaFieldIterator = docPartData.orderedMetaFieldIterator();
        while (metaFieldIterator.hasNext()) {
            MetaField metaField = metaFieldIterator.next();
            copyStatementBuilder.append("\"")
                .append(metaField.getIdentifier())
                .append("\",");
        }
        copyStatementBuilder.setCharAt(copyStatementBuilder.length() - 1, ')');
        copyStatementBuilder.append(" FROM STDIN");
        final String copyStatement = copyStatementBuilder.toString();
        return copyStatement;
    }

    private void addValuesToCopy(
            StringBuilder sb,
            DocPartRow docPartRow,
            Collection<InternalField<?>> internalFields) {
        for (InternalField<?> internalField : internalFields) {
            sb
                .append(internalField.<String>apply(docPartRow, internalValue -> 
                    { 
                        if (internalValue == null) { 
                            return "\\N"; 
                        } else {
                            return internalValue.toString();
                        }
                    }));
        }
        for (KVValue<?> value : docPartRow.getScalarValues()) {
            addValueToCopy(sb, value);
        }
        for (KVValue<?> value : docPartRow.getFieldValues()) {
            addValueToCopy(sb, value);
        }
        sb.setCharAt(sb.length() - 1, '\n');
    }

    protected void addValueToCopy(StringBuilder sb, KVValue<?> value) {
        if (value != null){
            value.accept(PostgreSQLValueToCopyConverter.INSTANCE, sb);
        } else {
            sb.append("\\N");
        }
        sb.append('\t');
    }

    private void executeCopy(CopyManager copyManager, String copyStatement, final StringBuilder sb) throws SQLException, IOException {
        Reader reader = new StringBuilderReader(sb);
        
        copyManager.copyIn(copyStatement, reader);
    }
    
    private static class StringBuilderReader extends Reader {

        private final StringBuilder sb;
        private int readerIndex = 0;

        public StringBuilderReader(StringBuilder sb) {
            this.sb = sb;
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (readerIndex == sb.length()) {
                return -1;
            }
            int newReaderIndex = Math.min(sb.length(), readerIndex + len);
            sb.getChars(readerIndex, newReaderIndex, cbuf, off);
            int diff = newReaderIndex - readerIndex;
            readerIndex = newReaderIndex;
            return diff;
        }

        @Override
        public void close() {
        }

    }

    @Override
    protected String getInsertDocPartDataStatement(String schemaName, MetaDocPart metaDocPart,
            Iterator<MetaField> metaFieldIterator, Iterator<MetaScalar> metaScalarIterator,
            Collection<InternalField<?>> internalFields, List<FieldType> fieldTypeList) {
        final StringBuilder insertStatementBuilder = new StringBuilder(2048);
        final StringBuilder insertStatementValuesBuilder = new StringBuilder(1024);
        insertStatementBuilder.append("INSERT INTO \"")
            .append(schemaName)
            .append("\".\"")
            .append(metaDocPart.getIdentifier())
            .append("\" (");
        insertStatementValuesBuilder.append(" VALUES (");
        for (InternalField<?> internalField : internalFields) {
            insertStatementBuilder.append("\"")
                .append(internalField.getName())
                .append("\",");
            insertStatementValuesBuilder.append("?,");
        }
        while (metaScalarIterator.hasNext()) {
            MetaScalar metaScalar = metaScalarIterator.next();
            insertStatementBuilder.append("\"")
                .append(metaScalar.getIdentifier())
                .append("\",");
            insertStatementValuesBuilder.append("?,");
            fieldTypeList.add(metaScalar.getType());
        }
        while (metaFieldIterator.hasNext()) {
            MetaField metaField = metaFieldIterator.next();
            insertStatementBuilder.append("\"")
                .append(metaField.getIdentifier())
                .append("\",");
            insertStatementValuesBuilder.append("?,");
            fieldTypeList.add(metaField.getType());
        }
        insertStatementBuilder.setCharAt(insertStatementBuilder.length() - 1, ')');
        insertStatementValuesBuilder.setCharAt(insertStatementValuesBuilder.length() - 1, ')');
        insertStatementBuilder.append(insertStatementValuesBuilder);
        
        String statement = insertStatementBuilder.toString();
        return statement;
    }
}