package com.torodb.backend.postgresql;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.Converter;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.torodb.backend.DbBackend;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.TableRefComparator;
import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.backend.index.NamedDbIndex;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.postgresql.converters.PostgreSQLValueToCopyConverter;
import com.torodb.backend.postgresql.converters.jooq.PostgreSQLValueToJooqDataTypeProvider;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaCollectionTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDatabaseTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDocPartTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaFieldTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaScalarTable;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.kvdocument.values.KVValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
@Singleton
public class PostgreSQLSqlInterface implements SqlInterface {
    private static final Logger LOGGER
        = LogManager.getLogger(PostgreSQLSqlInterface.class);

    private static final long serialVersionUID = 784618502;

    private static final char SEPARATOR = '_';
    private static final char ARRAY_DIMENSION_SEPARATOR = '$';
    private static final char[] FIELD_TYPE_IDENTIFIERS = new char[FieldType.values().length];
    private static final String[] SCALAR_FIELD_TYPE_IDENTIFIERS = new String[FieldType.values().length];
    static {
        FIELD_TYPE_IDENTIFIERS[FieldType.BINARY.ordinal()]='r'; // [r]aw
        FIELD_TYPE_IDENTIFIERS[FieldType.BOOLEAN.ordinal()]='b'; // [b]inary
        FIELD_TYPE_IDENTIFIERS[FieldType.DATE.ordinal()]='c'; // [c]alendar
        FIELD_TYPE_IDENTIFIERS[FieldType.DOUBLE.ordinal()]='d'; // [d]ouble
        FIELD_TYPE_IDENTIFIERS[FieldType.INSTANT.ordinal()]='g'; // [G]eorge Gamow or Admiral [G]race Hopper that were the earliest users of the term nanosecond
        FIELD_TYPE_IDENTIFIERS[FieldType.INTEGER.ordinal()]='i'; // [i]nteger
        FIELD_TYPE_IDENTIFIERS[FieldType.LONG.ordinal()]='l'; // [l]ong
        FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_OBJECT_ID.ordinal()]='x';
        FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_TIME_STAMP.ordinal()]='y';
        FIELD_TYPE_IDENTIFIERS[FieldType.NULL.ordinal()]='n'; // [n]ull
        FIELD_TYPE_IDENTIFIERS[FieldType.STRING.ordinal()]='s'; // [s]tring
        FIELD_TYPE_IDENTIFIERS[FieldType.TIME.ordinal()]='t'; // [t]ime
        FIELD_TYPE_IDENTIFIERS[FieldType.CHILD.ordinal()]='e'; // [e]lement
        
        Set<Character> fieldTypeIdentifierSet = new HashSet<>();
        for (FieldType fieldType : FieldType.values()) {
            if (FIELD_TYPE_IDENTIFIERS.length <= fieldType.ordinal()) {
                throw new SystemException("FieldType " + fieldType + " has not been mapped to an identifier.");
            }
            
            char identifier = FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()];
            
            if ((identifier < 'a' || identifier > 'z') &&
                    (identifier < '0' || identifier > '9')) {
                throw new SystemException("FieldType " + fieldType + " has an unallowed identifier " 
                        + identifier);
            }
            
            if (fieldTypeIdentifierSet.contains(identifier)) {
                throw new SystemException("FieldType " + fieldType + " identifier " 
                        + identifier + " was used by another FieldType.");
            }
            
            fieldTypeIdentifierSet.add(identifier);
            
            SCALAR_FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()] = DocPartTableFields.SCALAR.fieldName + SEPARATOR + identifier;
        }
    }

    private static final String[] RESTRICTED_SCHEMA_NAMES = new String[] {
            TorodbSchema.TORODB_SCHEMA,
            "pg_catalog",
            "information_schema",
            "pg_toast",
            "public" 
    };
    {
        Arrays.sort(RESTRICTED_SCHEMA_NAMES);
    }

    private static final String[] RESTRICTED_COLUMN_NAMES = new String[] {
            DocPartTableFields.DID.fieldName,
            DocPartTableFields.RID.fieldName,
            DocPartTableFields.PID.fieldName,
            DocPartTableFields.SEQ.fieldName,
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.BINARY.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.BOOLEAN.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.DATE.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.DOUBLE.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.INSTANT.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.INTEGER.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.LONG.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_OBJECT_ID.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_TIME_STAMP.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.NULL.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.STRING.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.TIME.ordinal()],
            SCALAR_FIELD_TYPE_IDENTIFIERS[FieldType.CHILD.ordinal()],
            "oid",
            "tableoid",
            "xmin",
            "xmax",
            "cmin",
            "cmax",
            "ctid",
    };
    {
        Arrays.sort(RESTRICTED_COLUMN_NAMES);
    }

    private final ValueToJooqDataTypeProvider valueToJooqDataTypeProvider;
    private final FieldComparator fieldComparator = new FieldComparator();
    private final PostgreSQLMetaDatabaseTable metaDatabaseTable;
    private final PostgreSQLMetaCollectionTable metaCollectionTable;
    private final PostgreSQLMetaDocPartTable metaDocPartTable;
    private final PostgreSQLMetaFieldTable metaFieldTable;
    private final PostgreSQLMetaScalarTable metaScalarTable;
    private final DbBackend dbBackend;
    
    @Inject
    public PostgreSQLSqlInterface(DbBackend dbBackend) {
        this.valueToJooqDataTypeProvider = PostgreSQLValueToJooqDataTypeProvider.getInstance();
        metaDatabaseTable = PostgreSQLMetaDatabaseTable.DATABASE;
        metaCollectionTable = PostgreSQLMetaCollectionTable.COLLECTION;
        metaDocPartTable = PostgreSQLMetaDocPartTable.DOC_PART;
        metaFieldTable = PostgreSQLMetaFieldTable.FIELD;
        metaScalarTable = PostgreSQLMetaScalarTable.SCALAR;
        this.dbBackend = dbBackend;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        //TODO: Try to remove make SqlInterface not serializable
        stream.defaultReadObject();
    }

    @Nonnull
    @Override
    public PostgreSQLMetaDatabaseTable getMetaDatabaseTable() {
        return metaDatabaseTable;
    }

    @Nonnull
    @Override
    public PostgreSQLMetaCollectionTable getMetaCollectionTable() {
        return metaCollectionTable;
    }

    @Nonnull
    @Override
    public PostgreSQLMetaDocPartTable getMetaDocPartTable() {
        return metaDocPartTable;
    }

    @Nonnull
    @Override
    public PostgreSQLMetaFieldTable getMetaFieldTable() {
        return metaFieldTable;
    }

    @Nonnull
    @Override
    public PostgreSQLMetaScalarTable getMetaScalarTable() {
        return metaScalarTable;
    }

    private Iterable<Field<?>> getFieldIterator(Iterable<Field<?>> fields) {
        List<Field<?>> fieldList = Lists.newArrayList(fields);
        Collections.sort(fieldList, fieldComparator);

        return fieldList;
    }

    private static class FieldComparator implements Comparator<Field>, Serializable {

        private static final List<Integer> sqlTypeOrder = Arrays.asList(new Integer[]{
                    java.sql.Types.NULL,
                    java.sql.Types.DOUBLE,
                    java.sql.Types.BIGINT,
                    java.sql.Types.INTEGER,
                    java.sql.Types.FLOAT,
                    java.sql.Types.TIME,
                    java.sql.Types.DATE,
                    java.sql.Types.REAL,
                    java.sql.Types.TINYINT,
                    java.sql.Types.CHAR,
                    java.sql.Types.BIT,
                    java.sql.Types.BINARY
                });
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Field o1, Field o2) {
            if (o1.getName().equals(DocPartTableFields.DID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.DID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.RID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.RID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.PID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.PID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.SEQ)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.SEQ)) {
                return 1;
            }

            int i1 = sqlTypeOrder.indexOf(o1.getDataType().getSQLType());
            int i2 = sqlTypeOrder.indexOf(o2.getDataType().getSQLType());

            if (i1 == i2) {
                return o1.getName().compareTo(o2.getName());
            }
            if (i1 == -1) {
                return 1;
            }
            if (i2 == -1) {
                return -1;
            }
            return i1 - i2;
        }

    }

    @Override
    public int identifierMaxSize() {
        return 63;
    }

    @Override
    public boolean isAllowedSchemaIdentifier(@Nonnull String schemaName) {
        return Arrays.binarySearch(RESTRICTED_SCHEMA_NAMES, schemaName) >= 0;
    }
    
    @Override
    public boolean isAllowedTableIdentifier(@Nonnull String columnName) {
        return true;
    }
    
    @Override
    public boolean isAllowedColumnIdentifier(@Nonnull String columnName) {
        return Arrays.binarySearch(RESTRICTED_COLUMN_NAMES, columnName) >= 0;
    }

    @Override
    public void createMetaDatabaseTable(DSLContext dsl) {
    	String schemaName = metaDatabaseTable.getSchema().getName();
    	String tableName = metaDatabaseTable.getName();
    	
        String statement = new StringBuilder()
        		 .append("CREATE TABLE ")
                 .append(fullTableName(schemaName, tableName))
                 .append(" (")
                 .append(MetaDatabaseTable.TableFields.NAME.name()).append("             varchar     PRIMARY KEY     ,")
                 .append(MetaDatabaseTable.TableFields.IDENTIFIER.name()).append("       varchar     NOT NULL UNIQUE ")
                 .append(")")
                .toString();
        executeStatement(dsl, statement, Context.ddl);
    }
    
    @Override
    public void createMetaCollectionTable(DSLContext dsl) {
    	String schemaName = metaCollectionTable.getSchema().getName();
    	String tableName = metaCollectionTable.getName();
    	String statement = new StringBuilder()
    			 .append("CREATE TABLE ")
                 .append(fullTableName(schemaName, tableName))
                 .append(" (")
                 .append(MetaCollectionTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                 .append(MetaCollectionTable.TableFields.NAME.name()).append("             varchar     NOT NULL        ,")
                 .append(MetaCollectionTable.TableFields.IDENTIFIER.name()).append("       varchar     NOT NULL UNIQUE ,")
                 .append("    PRIMARY KEY (").append(MetaCollectionTable.TableFields.DATABASE.name()).append(",")
                     .append(MetaCollectionTable.TableFields.NAME.name()).append(")")
                 .append(")")
                 .toString();
    	executeStatement(dsl, statement, Context.ddl);
    }

    @Override
    public void createMetaDocPartTable(DSLContext dsl) {
    	String schemaName = metaDocPartTable.getSchema().getName();
    	String tableName = metaDocPartTable.getName();
    	String statement = new StringBuilder()
    			.append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(MetaDocPartTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                .append(MetaDocPartTable.TableFields.COLLECTION.name()).append("       varchar     NOT NULL        ,")
                .append(MetaDocPartTable.TableFields.TABLE_REF.name()).append("        varchar[]   NOT NULL        ,")
                .append(MetaDocPartTable.TableFields.IDENTIFIER.name()).append("       varchar     NOT NULL        ,")
                .append(MetaDocPartTable.TableFields.LAST_RID.name()).append("         int         NOT NULL        ,")
                .append("    PRIMARY KEY (").append(MetaDocPartTable.TableFields.DATABASE.name()).append(",")
                    .append(MetaDocPartTable.TableFields.COLLECTION.name()).append(",")
                    .append(MetaDocPartTable.TableFields.TABLE_REF.name()).append("),")
                .append("    UNIQUE (").append(MetaDocPartTable.TableFields.DATABASE.name()).append(",")
                    .append(MetaDocPartTable.TableFields.IDENTIFIER.name()).append(")")
                .append(")")
                .toString();
    	executeStatement(dsl, statement, Context.ddl);    	
    }

    @Override
    public void createMetaFieldTable(DSLContext dsl) {
    	String schemaName = metaFieldTable.getSchema().getName();
    	String tableName = metaFieldTable.getName();
    	String statement = new StringBuilder()
    			.append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(MetaFieldTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                .append(MetaFieldTable.TableFields.COLLECTION.name()).append("       varchar     NOT NULL        ,")
                .append(MetaFieldTable.TableFields.TABLE_REF.name()).append("        varchar[]   NOT NULL        ,")
                .append(MetaFieldTable.TableFields.NAME.name()).append("             varchar     NOT NULL        ,")
                .append(MetaFieldTable.TableFields.TYPE.name()).append("             varchar     NOT NULL        ,")
                .append(MetaFieldTable.TableFields.IDENTIFIER.name()).append("       varchar     NOT NULL        ,")
                .append("    PRIMARY KEY (").append(MetaFieldTable.TableFields.DATABASE.name()).append(",")
                .append(MetaFieldTable.TableFields.COLLECTION.name()).append(",")
                .append(MetaFieldTable.TableFields.TABLE_REF.name()).append(",")
                .append(MetaFieldTable.TableFields.NAME.name()).append(",")                
                .append(MetaFieldTable.TableFields.TYPE.name()).append("),")
                .append("    UNIQUE (").append(MetaFieldTable.TableFields.DATABASE.name()).append(",")
                    .append(MetaFieldTable.TableFields.COLLECTION.name()).append(",")
                    .append(MetaFieldTable.TableFields.TABLE_REF.name()).append(",")
                    .append(MetaFieldTable.TableFields.IDENTIFIER.name()).append(")")
                .append(")")
                .toString();
        executeStatement(dsl, statement, Context.ddl);    	
    }

    @Override
    public void createMetaScalarTable(DSLContext dsl) {
        String schemaName = metaScalarTable.getSchema().getName();
        String tableName = metaScalarTable.getName();
        String statement = new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(MetaScalarTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                .append(MetaScalarTable.TableFields.COLLECTION.name()).append("       varchar     NOT NULL        ,")
                .append(MetaScalarTable.TableFields.TABLE_REF.name()).append("        varchar[]   NOT NULL        ,")
                .append(MetaScalarTable.TableFields.TYPE.name()).append("             varchar     NOT NULL        ,")
                .append(MetaScalarTable.TableFields.IDENTIFIER.name()).append("       varchar     NOT NULL        ,")
                .append("    PRIMARY KEY (").append(MetaScalarTable.TableFields.DATABASE.name()).append(",")
                .append(MetaScalarTable.TableFields.COLLECTION.name()).append(",")
                .append(MetaScalarTable.TableFields.TABLE_REF.name()).append(",")
                .append(MetaScalarTable.TableFields.TYPE.name()).append("),")
                .append("    UNIQUE (").append(MetaScalarTable.TableFields.DATABASE.name()).append(",")
                    .append(MetaScalarTable.TableFields.COLLECTION.name()).append(",")
                    .append(MetaScalarTable.TableFields.TABLE_REF.name()).append(",")
                    .append(MetaScalarTable.TableFields.IDENTIFIER.name()).append(")")
                .append(")")
                .toString();
        executeStatement(dsl, statement, Context.ddl);      
    }

    @Override
    public @Nonnull String createMetaIndexesTableStatement(
            @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn
    ) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(indexNameColumn).append("       varchar     PRIMARY KEY,")
                .append(indexOptionsColumn).append("    jsonb       NOT NULL")
                .append(")")
                .toString();
    }

    private static @Nonnull StringBuilder fullTableName(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(tableName).append("\"");
    }

    @Override
    public void deleteDocParts(@Nonnull DSLContext dsl,
            @Nonnull String schemaName, @Nonnull String tableName,
            @Nonnull List<Integer> dids
    ) {
        Preconditions.checkArgument(dids.size() > 0, "At least 1 did must be specified");
        
        StringBuilder sb = new StringBuilder()
                .append("DELETE FROM ")
                .append(fullTableName(schemaName, tableName))
                .append(" WHERE \"")
                .append(MetaDocPartTable.DocPartTableFields.DID.fieldName)
                .append("\" IN (");
        for (Integer did : dids) {
            sb.append(did)
                .append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        
        Connection connection = dsl.configuration().connectionProvider().acquire();
        
        try {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sb.toString())) {
                preparedStatement.executeUpdate();
            }
        } catch(SQLException ex) {
            handleRollbackException(Context.delete, ex);
            
            throw new SystemException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    @Override
    public void dropSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName) {
        String query = "DROP SCHEMA \"" + schemaName + "\" CASCADE";
        
        executeUpdate(dsl, query, Context.ddl);
    }

    @Nonnull
    @Override
    public DocPartResults<ResultSet> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, 
            @Nonnull Collection<Integer> dids) throws SQLException {
        Preconditions.checkArgument(dids.size() > 0, "At least 1 did must be specified");
        
        ImmutableList.Builder<DocPartResult<ResultSet>> docPartResultSetsBuilder = ImmutableList.builder();
        Connection connection = dsl.configuration().connectionProvider().acquire();
        Iterator<? extends MetaDocPart> metaDocPartIterator = metaCollection
                .streamContainedMetaDocParts()
                .sorted(TableRefComparator.MetaDocPart.DESC)
                .iterator();
        while (metaDocPartIterator.hasNext()) {
            MetaDocPart metaDocPart = metaDocPartIterator.next();
            StringBuilder sb = new StringBuilder()
                    .append("SELECT ");
            Collection<InternalField<?>> internalFields = getDocPartTableInternalFields(metaDocPart);
            for (InternalField<?> internalField : internalFields) {
                sb.append('"')
                    .append(internalField.getName())
                    .append("\",");
            }
            metaDocPart.streamScalars().forEach(metaScalar -> {
                sb.append('"')
                    .append(metaScalar.getIdentifier())
                    .append("\",");
            });
            metaDocPart.streamFields().forEach(metaField -> {
                sb.append('"')
                    .append(metaField.getIdentifier())
                    .append("\",");
            });
            sb.setCharAt(sb.length() - 1, ' ');
            sb
                .append("FROM \"")
                .append(metaDatabase.getIdentifier())
                .append("\".\"")
                .append(metaDocPart.getIdentifier())
                .append("\" WHERE \"")
                .append(metaDocPartTable.DID.getName())
                .append("\" IN (");
            Converter<?, Integer> converter = 
                    metaDocPartTable.DID.getDataType().getConverter();
            for (Integer requestedDoc : dids) {
                sb.append(converter.to(requestedDoc))
                    .append(',');
            }
            sb.setCharAt(sb.length() - 1, ')');
            if (!metaDocPart.getTableRef().isRoot()) {
                sb.append(" ORDER BY ");
                for (InternalField<?> internalField : internalFields) {
                    sb
                        .append('"')
                        .append(internalField.getName())
                        .append("\",");
                }
                sb.deleteCharAt(sb.length() - 1);
            }

            PreparedStatement preparedStatement = connection.prepareStatement(sb.toString());
            docPartResultSetsBuilder.add(new DocPartResult<ResultSet>(metaDocPart, preparedStatement.executeQuery()));
        }
        return new DocPartResults<ResultSet>(docPartResultSetsBuilder.build());
    }

    @Override
    public void createIndex(@Nonnull DSLContext dsl,
            @Nonnull String indexName, @Nonnull String schemaName, @Nonnull String tableName,
            @Nonnull String columnName, boolean ascending
    ) {
        StringBuilder sb = new StringBuilder()
                .append("CREATE INDEX ")
                .append("\"").append(indexName).append("\"")
                .append(" ON ")
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(tableName).append("\"")
                .append(" (")
                    .append("\"").append(columnName).append("\"")
                    .append(" ").append(ascending ? "ASC" : "DESC")
                .append(")");
        
        executeUpdate(dsl, sb.toString(), Context.ddl);
    }

    @Override
    public void dropIndex(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String indexName) {
        StringBuilder sb = new StringBuilder()
                .append("DROP INDEX ")
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(indexName).append("\"");
        
        executeUpdate(dsl, sb.toString(), Context.ddl);
    }

    @Override
    @SuppressFBWarnings(
            value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "False positive: https://sourceforge.net/p/findbugs/bugs/1021/")
    public long getDatabaseSize(
            @Nonnull DSLContext dsl,
            @Nonnull String databaseName
            ) {
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        
        try (PreparedStatement ps = connection.prepareStatement("SELECT * from pg_database_size(?)")) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
        finally {
            connectionProvider.release(connection);
        }
    }

    @Override
    @SuppressFBWarnings(
            value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "False positive: https://sourceforge.net/p/findbugs/bugs/1021/")
    public Long getCollectionSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schema,
            @Nonnull String collection
            ) {
        ConnectionProvider connectionProvider 
                = dsl.configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();

        String query = "SELECT sum(table_size)::bigint "
                + "FROM ("
                + "  SELECT "
                + "    pg_relation_size(pg_catalog.pg_class.oid) as table_size "
                + "  FROM pg_catalog.pg_class "
                + "    JOIN pg_catalog.pg_namespace "
                + "       ON relnamespace = pg_catalog.pg_namespace.oid "
                + "    WHERE pg_catalog.pg_namespace.nspname = ?"
                + ") AS t";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, schema);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            connectionProvider.release(connection);
        }
    }

    @Override
    public void createSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName){
    	String query = "CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\"";
    	executeUpdate(dsl, query, Context.ddl);
    }

    @Override
    @SuppressFBWarnings(
            value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "False positive: https://sourceforge.net/p/findbugs/bugs/1021/")
    public Long getDocumentsSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schema,
            String collection
            ) {
        ConnectionProvider connectionProvider 
                = dsl.configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        String query = "SELECT sum(table_size)::bigint from ("
                    + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                    + "FROM pg_class join pg_tables on pg_class.relname = pg_tables.tablename "
                    + "where pg_tables.schemaname = ? "
                    + "   and pg_tables.tablename LIKE 't_%'"
                    + ") as t";
        
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, schema);
            ResultSet rs = ps.executeQuery();
            rs.next();
            
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            connectionProvider.release(connection);
        }
    }

    @Override
    public Long getIndexSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schema,
            String collection, String index,
            Set<NamedDbIndex> relatedDbIndexes,
            Map<String, Integer> relatedToroIndexes
            ) {
        ConnectionProvider connectionProvider 
                = dsl.configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        
        String query = "SELECT sum(table_size)::bigint from ("
                + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                + "FROM pg_class join pg_indexes "
                + "  on pg_class.relname = pg_indexes.tablename "
                + "WHERE pg_indexes.schemaname = ? "
                + "  and pg_indexes.indexname = ?"
                + ") as t";

        long result = 0;
        try {
            
            for (NamedDbIndex dbIndex : relatedDbIndexes) {
                try (PreparedStatement ps = connection.prepareStatement(query)) {
                    ps.setString(1, schema);
                    ps.setString(2, dbIndex.getName());
                    ResultSet rs = ps.executeQuery();
                    assert relatedToroIndexes.containsKey(dbIndex.getName());
                    int usedBy = relatedToroIndexes.get(dbIndex.getName());
                    assert usedBy != 0;
                    rs.next();
                    result += rs.getLong(1) / usedBy;
                }
            }
            return result;
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            connectionProvider.release(connection);
        }
    }
    
    @Override
    public void insertDocPartData(DSLContext dsl, String schemaName, DocPartData docPartData) {
    	if (docPartData.rowCount()==0){
    		return;
    	}
        Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
            int maxCappedSize = 10;
            int cappedSize = Math.min(docPartData.rowCount(), maxCappedSize);

            if (cappedSize < maxCappedSize) { //there are not enough elements on the insert => fallback
                LOGGER.debug(
                        "The insert window is not big enough to use copy (the "
                                + "limit is {}, the real size is {}).",
                        maxCappedSize,
                        cappedSize
                );
                standardInsertPathDocuments(dsl, schemaName, docPartData);
            }
            else {
                if (!connection.isWrapperFor(PGConnection.class)) {
                    LOGGER.warn("It was impossible to use the PostgreSQL way to "
                            + "insert documents. Inserting using the standard "
                            + "implementation");
                    standardInsertPathDocuments(dsl, schemaName, docPartData);
                }
                else {
                    copyInsertPathDocuments(
                            connection.unwrap(PGConnection.class),
                            schemaName,
                            docPartData
                    );
                }
            }
        } catch (DataAccessException ex) {
            handleRollbackException(Context.insert, ex);
            throw new SystemException(ex);
        } catch (SQLException ex) {
            handleRollbackException(Context.insert, ex);
            throw new SystemException(ex);
        } catch (IOException ex) {
            throw new SystemException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    protected void standardInsertPathDocuments(DSLContext dsl, String schemaName, DocPartData docPartData) {
        throw new UnsupportedOperationException();
    }
    
    private void copyInsertPathDocuments(
            PGConnection connection,
            String schemaName,
            DocPartData docPartData) throws SQLException, IOException {

        final int maxBatchSize = 1000;
        final StringBuilder sb = new StringBuilder(2048);
        final StringBuilder copyStatementBuilder = new StringBuilder();
        final CopyManager copyManager = connection.getCopyAPI();
        final MetaDocPart metaDocPart = docPartData.getMetaDocPart();
        copyStatementBuilder.append("COPY \"")
            .append(schemaName)
            .append("\".\"").append(metaDocPart.getIdentifier())
            .append("\"").append(" (");
        
        Collection<InternalField<?>> internalFields = getDocPartTableInternalFields(metaDocPart);
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
        
        Iterator<DocPartRow> docPartRowIterator = docPartData.iterator();
        int docCounter = 0;
        while (docPartRowIterator.hasNext()) {
            DocPartRow tableRow = docPartRowIterator.next();
            docCounter++;

            addToCopy(sb, tableRow, internalFields);
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

    private void addToCopy(
            StringBuilder sb,
            DocPartRow docPartRow,
            Collection<InternalField<?>> internalFields) {
        for (InternalField<?> internalField : internalFields) {
            sb
                .append(internalField.<String>apply(docPartRow, internalValue -> { if (internalValue == null) return "\\N"; else return internalValue.toString(); }));
        }
        for (KVValue<?> value : docPartRow.getScalarValues()) {
            if (value!=null){
                value.accept(PostgreSQLValueToCopyConverter.INSTANCE, sb);
            }else{
                sb.append("\\N");
            }
            sb.append('\t');
        }
        for (KVValue<?> value : docPartRow.getFieldValues()) {
            if (value!=null){
                value.accept(PostgreSQLValueToCopyConverter.INSTANCE, sb);
            }else{
                sb.append("\\N");
            }
            sb.append('\t');
        }
        sb.setCharAt(sb.length() - 1, '\n');
    }

    protected Integer translateSubDocIndexToDatabase(int index) {
        if (index == 0) {
            return null;
        }
        return index;
    }

    private void executeCopy(CopyManager copyManager, String copyStatement, final StringBuilder sb) throws SQLException, IOException {
        Reader reader = new StringBuilderReader(sb);
        
        copyManager.copyIn(copyStatement, reader);
    }
    
//    public interface PGConnection extends Connection {
//        public CopyManager getCopyAPI();
//    }
//    
//    public interface CopyManager {
//        public void copyIn(String copyStatement, Reader reader);
//    }

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
    public int consumeRids(DSLContext dsl, String database, String collection, TableRef tableRef, int count) {
        Record1<Integer> lastRid = dsl.select(metaDocPartTable.LAST_RID).from(metaDocPartTable).where(
                metaDocPartTable.DATABASE.eq(database)
                .and(metaDocPartTable.COLLECTION.eq(collection))
                .and(metaDocPartTable.TABLE_REF.eq(TableRefConverter.toStringArray(tableRef))))
            .fetchOne();
        dsl.update(metaDocPartTable).set(metaDocPartTable.LAST_RID, metaDocPartTable.LAST_RID.plus(count)).where(
                metaDocPartTable.DATABASE.eq(database)
                .and(metaDocPartTable.COLLECTION.eq(collection))
                .and(metaDocPartTable.TABLE_REF.eq(TableRefConverter.toStringArray(tableRef)))).execute();
        return lastRid.value1();
    }

    @Override
    public DataTypeForKV<?> getDataType(FieldType type) {
        return valueToJooqDataTypeProvider.getDataType(type);
    }

    private static final String[] ROLLBACK_EXCEPTIONS = new String[] { "40001", "40P01" };
    {
        Arrays.sort(ROLLBACK_EXCEPTIONS);
    }
    
    @Override
    public void handleRollbackException(Context context, SQLException sqlException) {
        if (Arrays.binarySearch(ROLLBACK_EXCEPTIONS, sqlException.getSQLState()) >= 0) {
            throw new RollbackException(sqlException);
        }
    }

    @Override
    public void handleRollbackException(Context context, DataAccessException dataAccessException) {
        if (Arrays.binarySearch(ROLLBACK_EXCEPTIONS, dataAccessException.sqlState()) >= 0) {
            throw new RollbackException(dataAccessException);
        }
    }

    @Override
    public boolean isSameIdentifier(String leftIdentifier, String rightIdentifier) {
        return leftIdentifier.equals(rightIdentifier);
    }

    @Override
    public Collection<InternalField<?>> getDocPartTableInternalFields(MetaDocPart metaDocPart) {
        TableRef tableRef = metaDocPart.getTableRef();
        if (tableRef.isRoot()) {
            return metaDocPartTable.ROOT_FIELDS;
        } else if (tableRef.getParent().get().isRoot()) {
            return metaDocPartTable.FIRST_FIELDS;
        }
        return metaDocPartTable.FIELDS;
    }
    
	@Override
	public int getLastRowIdUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart) {
		
		TableRef tableRef = metaDocPart.getTableRef();
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT max(\"")
		  .append(getColumnIdByTableRefLevel(tableRef))
		  .append("\") FROM \"")
		  .append(metaDatabase.getIdentifier())
		  .append("\".\"")
		  .append(metaDocPart.getIdentifier())
		  .append("\"");
		
		Connection connection = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sb.toString())){
        	ResultSet rs = preparedStatement.executeQuery();
        	rs.next();
        	int maxId = rs.getInt(1);
        	if (rs.wasNull()){
        		return -1;
        	}
        	return maxId;
        } catch (SQLException ex){
        	handleRollbackException(Context.ddl, ex);
            throw new SystemException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
	}
	
	private String getColumnIdByTableRefLevel(TableRef tableRef){
		 if (tableRef.isRoot()){
			 return DocPartTableFields.DID.fieldName;
        }
		 return DocPartTableFields.RID.fieldName;
	}
	
	@Override
	public void addMetaDatabase(DSLContext dsl, String databaseName, String databaseIdentifier) {
		Query query = dsl.insertInto(metaDatabaseTable)
	            .set(metaDatabaseTable.newRecord().values(databaseName, databaseIdentifier));
	        executeQuery(query, Context.ddl);
	}

	@Override
	public void addMetaCollection(DSLContext dsl, String databaseName, String collectionName, String collectionIdentifier) {
		Query query = dsl.insertInto(metaCollectionTable)
	            .set(metaCollectionTable.newRecord()
	            .values(databaseName, collectionName, collectionIdentifier));
	        executeQuery(query, Context.ddl);
	}
	
	@Override
	public void addMetaDocPart(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String docPartIdentifier) {
		Query query = dsl.insertInto(metaDocPartTable)
	            .set(metaDocPartTable.newRecord()
	            .values(databaseName, collectionName, tableRef, docPartIdentifier));
			executeQuery(query, Context.ddl);
	}
	
    @Override
	public void createDocPartTable(DSLContext dsl, String schemaName, String tableName, Collection<Field<?>> fields) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ")
		  .append(fullTableName(schemaName, tableName))
		  .append(" (");
		Configuration conf = dsl.configuration();
		int cont = 0;
		for (Field<?> field : getFieldIterator(fields)) {
			if (cont > 0) {
				sb.append(',');
			}
			sb.append('"').append(field.getName()).append("\" ").append(field.getDataType().getCastTypeName(conf));
			cont++;
		}
		sb.append(')');
		executeStatement(dsl, sb.toString(), Context.ddl);
	}

	@Override
	public void addMetaField(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String fieldName, String fieldIdentifier, FieldType type) {
		Query query = dsl.insertInto(metaFieldTable)
				.set(metaFieldTable.newRecord()
				.values(databaseName, collectionName, tableRef, fieldName, type, fieldIdentifier));
		executeQuery(query, Context.ddl);
	}
	
	@Override
    public void addColumnToDocPartTable(DSLContext dsl, String schemaName, String tableName, Field<?> field) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ")
	        .append(fullTableName(schemaName, tableName))
	        .append(" ADD COLUMN \"")
	        .append(field.getName())
	        .append("\" ")
            .append(field.getDataType().getCastTypeName(dsl.configuration()));
        executeStatement(dsl, sb.toString(), Context.ddl);
    }
	
    private void executeStatement(DSLContext dsl, String statement, Context context){
    	Connection c = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement ps = c.prepareStatement(statement)) {
            ps.execute();
        } catch (SQLException ex) {
        	handleRollbackException(context, ex);
            throw new SystemException(ex);
		} finally {
            dsl.configuration().connectionProvider().release(c);
        }    	
    }
    
    private void executeUpdate(DSLContext dsl, String statement, Context context){
    	Connection c = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement ps = c.prepareStatement(statement)) {
            ps.execute();
        } catch (SQLException ex) {
        	handleRollbackException(context, ex);
            throw new SystemException(ex);
		} finally {
            dsl.configuration().connectionProvider().release(c);
        }    	
    }
    
    private void executeQuery(Query query, Context context){
        try {
            query.execute();
        } catch (DataAccessException ex) {
        	handleRollbackException(context, ex);
            throw new SystemException(ex);
        }    	
    }

    @Override
    public void handleUserAndRetryException(Context context, SQLException sqlException) throws UserException {
        handleRollbackException(context, sqlException);
    }

    @Override
    public void handleUserAndRetryException(Context context, DataAccessException dataAccessException)
            throws UserException {
        handleRollbackException(context, dataAccessException);
    }

    @Override
    public char getSeparator() {
        return SEPARATOR;
    }

    @Override
    public char getArrayDimensionSeparator() {
        return ARRAY_DIMENSION_SEPARATOR;
    }

    @Override
    public char getFieldTypeIdentifier(FieldType fieldType) {
        return FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()];
    }

    @Override
    public String getScalarIdentifier(FieldType fieldType) {
        return SCALAR_FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()];
    }

    @Override
    public Connection createReadOnlyConnection() {
        try {
            Connection connection = dbBackend.getGlobalCursorDatasource().getConnection();
            return connection;
        } catch (SQLException ex) {
            handleRollbackException(Context.get_connection, ex);
            throw new SystemException("It was impossible to create a read only connection", ex);
        }
    }

    @Override
    public Connection createWriteConnection() {
        try {
            Connection connection = dbBackend.getSessionDataSource().getConnection();
            return connection;
        } catch (SQLException ex) {
            handleRollbackException(Context.get_connection, ex);
            throw new SystemException("It was impossible to create a write connection", ex);
        }
    }

    @Override
    public Connection createSystemConnection() {
        try {
            Connection connection = dbBackend.getSystemDataSource().getConnection();
            return connection;
        } catch (SQLException ex) {
            handleRollbackException(Context.get_connection, ex);
            throw new SystemException("It was impossible to create a system connection", ex);
        }
    }

    @Override
    public DSLContext createDSLContext(Connection connection) {
        return DSL.using(connection, SQLDialect.POSTGRES_9_5);
    }

    @Override
    public void addMetaScalar(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
            String fieldIdentifier, FieldType type) {
        // TODO Auto-generated method stub
        
    }
}
