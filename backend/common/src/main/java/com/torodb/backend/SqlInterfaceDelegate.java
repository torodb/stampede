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

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.index.NamedDbIndex;
import com.torodb.backend.tables.*;
import com.torodb.backend.tables.records.*;
import com.torodb.core.TableRef;
import com.torodb.core.backend.DidCursor;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;

public class SqlInterfaceDelegate implements SqlInterface {

    private static final long serialVersionUID = 1L;
    
    private final MetaDataReadInterface metaDataReadInterface;
    private final MetaDataWriteInterface metaDataWriteInterface;
    private final DataTypeProvider dataTypeProvider;
    private final StructureInterface structureInterface;
    private final ReadInterface readInterface;
    private final WriteInterface writeInterface;
    private final IdentifierConstraints identifierConstraints;
    private final ErrorHandler errorHandler;
    private final DslContextFactory dslContextFactory;
    private final DbBackend dbBackend;
    
    @Inject
    public SqlInterfaceDelegate(MetaDataReadInterface metaDataReadInterface,
            MetaDataWriteInterface metaDataWriteInterface, DataTypeProvider dataTypeProvider,
            StructureInterface structureInterface, ReadInterface readInterface, WriteInterface writeInterface,
            IdentifierConstraints identifierConstraints, ErrorHandler errorHandler,
            DslContextFactory dslContextFactory, DbBackend dbBackend) {
        super();
        this.metaDataReadInterface = metaDataReadInterface;
        this.metaDataWriteInterface = metaDataWriteInterface;
        this.dataTypeProvider = dataTypeProvider;
        this.structureInterface = structureInterface;
        this.readInterface = readInterface;
        this.writeInterface = writeInterface;
        this.identifierConstraints = identifierConstraints;
        this.errorHandler = errorHandler;
        this.dslContextFactory = dslContextFactory;
        this.dbBackend = dbBackend;
    }
    public <R extends MetaDatabaseRecord> MetaDatabaseTable<R> getMetaDatabaseTable() {
        return metaDataReadInterface.getMetaDatabaseTable();
    }
    public <R extends MetaCollectionRecord> MetaCollectionTable<R> getMetaCollectionTable() {
        return metaDataReadInterface.getMetaCollectionTable();
    }
    public <T, R extends MetaDocPartRecord<T>> MetaDocPartTable<T, R> getMetaDocPartTable() {
        return metaDataReadInterface.getMetaDocPartTable();
    }
    public <T, R extends MetaFieldRecord<T>> MetaFieldTable<T, R> getMetaFieldTable() {
        return metaDataReadInterface.getMetaFieldTable();
    }
    public <T, R extends MetaScalarRecord<T>> MetaScalarTable<T, R> getMetaScalarTable() {
        return metaDataReadInterface.getMetaScalarTable();
    }
    public Collection<InternalField<?>> getDocPartTableInternalFields(MetaDocPart metaDocPart) {
        return metaDataReadInterface.getDocPartTableInternalFields(metaDocPart);
    }
    public long getDatabaseSize(DSLContext dsl, String databaseName) {
        return metaDataReadInterface.getDatabaseSize(dsl, databaseName);
    }
    public Long getCollectionSize(DSLContext dsl, String schema, String collection) {
        return metaDataReadInterface.getCollectionSize(dsl, schema, collection);
    }
    public Long getDocumentsSize(DSLContext dsl, String schema, String collection) {
        return metaDataReadInterface.getDocumentsSize(dsl, schema, collection);
    }
    public Long getIndexSize(DSLContext dsl, String schema, String collection, String index,
            Set<NamedDbIndex> relatedDbIndexes, Map<String, Integer> relatedToroIndexes) {
        return metaDataReadInterface.getIndexSize(dsl, schema, collection, index, relatedDbIndexes, relatedToroIndexes);
    }
    public String createMetaIndexesTableStatement(String schemaName, String tableName, String indexNameColumn,
            String indexOptionsColumn) {
        return metaDataWriteInterface.createMetaIndexesTableStatement(schemaName, tableName, indexNameColumn,
                indexOptionsColumn);
    }
    public void createMetaDatabaseTable(DSLContext dsl) {
        metaDataWriteInterface.createMetaDatabaseTable(dsl);
    }
    public void createMetaCollectionTable(DSLContext dsl) {
        metaDataWriteInterface.createMetaCollectionTable(dsl);
    }
    public void createMetaDocPartTable(DSLContext dsl) {
        metaDataWriteInterface.createMetaDocPartTable(dsl);
    }
    public void createMetaFieldTable(DSLContext dsl) {
        metaDataWriteInterface.createMetaFieldTable(dsl);
    }
    public void createMetaScalarTable(DSLContext dsl) {
        metaDataWriteInterface.createMetaScalarTable(dsl);
    }
    public void addMetaDatabase(DSLContext dsl, String databaseName, String databaseIdentifier) {
        metaDataWriteInterface.addMetaDatabase(dsl, databaseName, databaseIdentifier);
    }
    public void addMetaCollection(DSLContext dsl, String databaseName, String collectionName,
            String collectionIdentifier) {
        metaDataWriteInterface.addMetaCollection(dsl, databaseName, collectionName, collectionIdentifier);
    }
    public void addMetaDocPart(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
            String docPartIdentifier) {
        metaDataWriteInterface.addMetaDocPart(dsl, databaseName, collectionName, tableRef, docPartIdentifier);
    }
    public void addMetaField(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
            String fieldName, String fieldIdentifier, FieldType type) {
        metaDataWriteInterface.addMetaField(dsl, databaseName, collectionName, tableRef, fieldName, fieldIdentifier,
                type);
    }
    public void addMetaScalar(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
            String fieldIdentifier, FieldType type) {
        metaDataWriteInterface.addMetaScalar(dsl, databaseName, collectionName, tableRef, fieldIdentifier, type);
    }
    public int consumeRids(DSLContext dsl, String database, String collection, TableRef tableRef, int count) {
        return metaDataWriteInterface.consumeRids(dsl, database, collection, tableRef, count);
    }
    public DataTypeForKV<?> getDataType(FieldType type) {
        return dataTypeProvider.getDataType(type);
    }
    public SQLDialect getDialect() {
        return dataTypeProvider.getDialect();
    }
    public void createSchema(DSLContext dsl, String schemaName) {
        structureInterface.createSchema(dsl, schemaName);
    }
    public void dropSchema(DSLContext dsl, String schemaName) {
        structureInterface.dropSchema(dsl, schemaName);
    }
    public void createDocPartTable(DSLContext dsl, String schemaName, String tableName, Collection<? extends Field<?>> fields) {
        structureInterface.createDocPartTable(dsl, schemaName, tableName, fields);
    }
    public void addColumnToDocPartTable(DSLContext dsl, String schemaName, String tableName, Field<?> field) {
        structureInterface.addColumnToDocPartTable(dsl, schemaName, tableName, field);
    }
    public void createIndex(DSLContext dsl, String fullIndexName, String tableSchema, String tableName,
            String tableColumnName, boolean isAscending) {
        structureInterface.createIndex(dsl, fullIndexName, tableSchema, tableName, tableColumnName, isAscending);
    }
    public void dropIndex(DSLContext dsl, String schemaName, String indexName) {
        structureInterface.dropIndex(dsl, schemaName, indexName);
    }
    public ResultSet getCollectionDidsWithFieldEqualsTo(DSLContext dsl, MetaDatabase metaDatabase,
            MetaDocPart metaDocPart, MetaField metaField, KVValue<?> value)
            throws SQLException {
        return readInterface.getCollectionDidsWithFieldEqualsTo(dsl, metaDatabase,  
                metaDocPart, metaField, value);
    }
    public ResultSet getAllCollectionDids(DSLContext dsl, MetaDatabase metaDatabase, MetaDocPart metaDocPart)
            throws SQLException {
        return readInterface.getAllCollectionDids(dsl, metaDatabase, metaDocPart);
    }
    public DocPartResults<ResultSet> getCollectionResultSets(DSLContext dsl, MetaDatabase metaDatabase,
            MetaCollection metaCollection, DidCursor didCursor, int maxSize) throws SQLException {
        return readInterface.getCollectionResultSets(dsl, metaDatabase, metaCollection, didCursor, maxSize);
    }
    public int getLastRowIdUsed(DSLContext dsl, MetaDatabase metaDatabase, MetaCollection metaCollection,
            MetaDocPart metaDocPart) {
        return readInterface.getLastRowIdUsed(dsl, metaDatabase, metaCollection, metaDocPart);
    }
    public void insertDocPartData(DSLContext dsl, String schemaName, DocPartData docPartData) {
        writeInterface.insertDocPartData(dsl, schemaName, docPartData);
    }
    public void deleteDocParts(DSLContext dsl, String schemaName, MetaCollection metaCollection, List<Integer> dids){
        writeInterface.deleteDocParts(dsl, schemaName, metaCollection, dids);
    }
    public int identifierMaxSize() {
        return identifierConstraints.identifierMaxSize();
    }
    public boolean isAllowedSchemaIdentifier(String identifier) {
        return identifierConstraints.isAllowedSchemaIdentifier(identifier);
    }
    public boolean isAllowedTableIdentifier(String identifier) {
        return identifierConstraints.isAllowedTableIdentifier(identifier);
    }
    public boolean isAllowedColumnIdentifier(String identifier) {
        return identifierConstraints.isAllowedColumnIdentifier(identifier);
    }
    public boolean isSameIdentifier(String leftIdentifier, String rightIdentifier) {
        return identifierConstraints.isSameIdentifier(leftIdentifier, rightIdentifier);
    }
    public char getSeparator() {
        return identifierConstraints.getSeparator();
    }
    public char getArrayDimensionSeparator() {
        return identifierConstraints.getArrayDimensionSeparator();
    }
    public char getFieldTypeIdentifier(FieldType fieldType) {
        return identifierConstraints.getFieldTypeIdentifier(fieldType);
    }
    public String getScalarIdentifier(FieldType fieldType) {
        return identifierConstraints.getScalarIdentifier(fieldType);
    }
    public void handleRollbackException(Context context, SQLException sqlException) {
        errorHandler.handleRollbackException(context, sqlException);
    }
    public void handleRollbackException(Context context, DataAccessException dataAccessException) {
        errorHandler.handleRollbackException(context, dataAccessException);
    }
    public void handleUserAndRetryException(Context context, SQLException sqlException) throws UserException {
        errorHandler.handleUserAndRetryException(context, sqlException);
    }
    public void handleUserAndRetryException(Context context, DataAccessException dataAccessException)
            throws UserException {
        errorHandler.handleUserAndRetryException(context, dataAccessException);
    }
    public DSLContext createDSLContext(Connection connection) {
        return dslContextFactory.createDSLContext(connection);
    }
    public DataSource getSessionDataSource() {
        return dbBackend.getSessionDataSource();
    }
    public DataSource getSystemDataSource() {
        return dbBackend.getSystemDataSource();
    }
    public DataSource getGlobalCursorDatasource() {
        return dbBackend.getGlobalCursorDatasource();
    }
    public long getDefaultCursorTimeout() {
        return dbBackend.getDefaultCursorTimeout();
    }
    public Connection createSystemConnection() {
        return dbBackend.createSystemConnection();
    }
    public Connection createReadOnlyConnection() {
        return dbBackend.createReadOnlyConnection();
    }
    public Connection createWriteConnection() {
        return dbBackend.createWriteConnection();
    }
}
