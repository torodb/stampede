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

package com.torodb.torod.db.wrappers.postgresql.meta;

import com.google.common.collect.MapMaker;
import com.google.common.io.CharStreams;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.db.wrappers.DatabaseInterface;
import com.torodb.torod.db.wrappers.exceptions.InvalidCollectionSchemaException;
import com.torodb.torod.db.wrappers.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.wrappers.tables.CollectionsTable;
import com.torodb.torod.db.wrappers.tables.records.CollectionsRecord;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class TorodbMeta {

    private final String databaseName;
    private final ConcurrentMap<String, CollectionSchema> collectionSchemes;
    private static final Logger LOGGER = LoggerFactory.getLogger(TorodbMeta.class);
    private final DatabaseInterface databaseInterface;

    @Inject
    public TorodbMeta(
            String databaseName,
            DSLContext dsl,
            DatabaseInterface databaseInterface
    ) throws SQLException, IOException, InvalidDatabaseException {
        this.databaseName = databaseName;
        this.databaseInterface = databaseInterface;

        Meta jooqMeta = dsl.meta();
        Connection conn = dsl.configuration().connectionProvider().acquire();
        DatabaseMetaData jdbcMeta = conn.getMetaData();

        //only system executor thread can update this map
        collectionSchemes = new MapMaker().concurrencyLevel(1).makeMap();

        TorodbSchema.TORODB.checkOrCreate(dsl, jooqMeta, jdbcMeta, databaseInterface);
        loadAllCollectionSchemas(dsl, jooqMeta, jdbcMeta);
        
        createTypes(conn, jdbcMeta);
        createProcedures(conn, jdbcMeta);
        createCast(conn, jdbcMeta);

        dsl.configuration().connectionProvider().release(conn);
    }
    
    private void loadAllCollectionSchemas(
            DSLContext dsl,
            Meta jooqMeta,
            DatabaseMetaData jdbcMeta) throws InvalidCollectionSchemaException {
        
        Result<CollectionsRecord> records
                = dsl.selectFrom(CollectionsTable.COLLECTIONS).fetch();
        
        for (CollectionsRecord colRecord : records) {
            CollectionSchema colSchema = new CollectionSchema(
                    colRecord.getSchema(), 
                    colRecord.getName(),
                    dsl, 
                    jdbcMeta, 
                    jooqMeta, 
                    this,
                    databaseInterface
            );
            collectionSchemes.put(colSchema.getCollection(), colSchema);
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public boolean exists(String collection) {
        return collectionSchemes.containsKey(collection);
    }

    public CollectionSchema getCollectionSchema(String collection) {
        CollectionSchema schema = collectionSchemes.get(collection);
        if (schema == null) {
            throw new IllegalArgumentException("There is no schema associated with collection "
                    + collection);
        }
        return schema;
    }

    public void dropCollectionSchema(String collection) {
        CollectionSchema removed = collectionSchemes.remove(collection);
        if (removed == null) {
            throw new IllegalArgumentException("Collection " + collection
                    + " didn't exist");
        }
    }

    public CollectionSchema createCollectionSchema(
            String colName,
            String schemaName,
            DSLContext dsl) throws InvalidCollectionSchemaException {
        if (collectionSchemes.containsKey(colName)) {
            throw new IllegalArgumentException("Collection '" + colName
                    + "' is already associated with a collection schema");
        }
        CollectionSchema result = new CollectionSchema(schemaName, colName, dsl, this, databaseInterface);
        collectionSchemes.put(colName, result);

        return result;
    }

    public Collection<CollectionSchema> getCollectionSchemes() {
        return Collections.unmodifiableCollection(collectionSchemes.values());
    }

    private void createTypes(
            Connection conn,
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        boolean findDocTypeExists = false;
        boolean twelveBytesExists = false;
        boolean torodbPatternExists = false;
        
        ResultSet typeInfo = null;
        try {
        
            typeInfo = jdbcMeta.getTypeInfo();
            while (typeInfo.next()) {
                findDocTypeExists = 
                        findDocTypeExists 
                        || typeInfo.getString("TYPE_NAME").equals("find_doc_type");
                twelveBytesExists = 
                        twelveBytesExists
                        || typeInfo.getString("TYPE_NAME").equals("twelve_bytes");
                torodbPatternExists = 
                        torodbPatternExists
                        || typeInfo.getString("TYPE_NAME").equals("torodb_pattern");
                
                if (findDocTypeExists && twelveBytesExists && torodbPatternExists) {
                    break;
                }
            }
        } finally {
            if (typeInfo != null) {
                typeInfo.close();
            }
        }
        
        if (!findDocTypeExists) {
            LOGGER.debug("Creating type find_doc_type");
            createFindDocType(conn);
            LOGGER.debug("Created type find_doc_type");
        }
        else {
            LOGGER.debug("Type find_doc_type found");
        }
        if (!twelveBytesExists) {
            LOGGER.debug("Creating type twelve_bytes");
            createTwelveBytesType(conn);
            LOGGER.debug("Created type twelve_bytes");
        }
        else {
            LOGGER.debug("Type twelve_bytes found");
        }
        if (!torodbPatternExists) {
            LOGGER.debug("Creating type torodb_pattern");
            createTorodbPatternType(conn);
            LOGGER.debug("Created type torodb_pattern");
        }
        else {
            LOGGER.debug("Type torodb_pattern found");
        }
    }

    private void createProcedures(
            Connection conn,
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createFindDocProcedure(conn, jdbcMeta);
        createFindDocsProcedure(conn, jdbcMeta);
        createFirstFreeDocIdProcedure(conn, jdbcMeta);
        createReserveDocIdsProcedure(conn, jdbcMeta);
        createVarcharToJsonbProcedure(conn, jdbcMeta);
    }

    private void createFindDocType(Connection conn) throws IOException, SQLException {
        executeSql(conn, "/sql/find_doc_type.sql");
    }

    private void createTwelveBytesType(Connection conn) throws IOException, SQLException {
        executeSql(conn, "/sql/twelve_bytes_type.sql");
    }

    private void createTorodbPatternType(Connection conn) throws IOException, SQLException {
        executeSql(conn, "/sql/torodb_pattern_type.sql");
    }
    
    private void createFindDocProcedure(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "find_doc");
    }
    
    private void createFindDocsProcedure(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "find_docs");
    }
    
    private void createFirstFreeDocIdProcedure(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "first_free_doc_id");
    }
    
    private void createReserveDocIdsProcedure(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "reserve_doc_ids");
    }
    
    private void createVarcharToJsonbProcedure(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "varchar_to_jsonb");
    }
    
    private void createProcedure(
            Connection conn,
            DatabaseMetaData jdbcMeta,
            String proc
    ) throws SQLException, IOException {
        if (!checkIfProcedureExists(jdbcMeta, proc)) {
            LOGGER.debug("Creating procedure "+proc);
            executeSql(conn, "/sql/"+proc+".sql");
            LOGGER.debug("Procedure "+proc+" created");
        }
        else {
            LOGGER.debug("Procedure "+proc+" found");
        }
        
    }
    
    private boolean checkIfProcedureExists(
            DatabaseMetaData jdbcMeta, 
            String procedureName
    ) throws SQLException {
        ResultSet procedures = null;
        try {
            procedures = jdbcMeta.getProcedures(null, "torodb", procedureName);
            
            return procedures.next();
        } finally {
            if (procedures != null) {
                procedures.close();
            }
        }
    }
    
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    private void executeSql(
            Connection conn, 
            String resourcePath
    ) throws IOException, SQLException {
        InputStream resourceAsStream
                = TorodbMeta.class.getResourceAsStream(resourcePath);
        if (resourceAsStream == null) {
            throw new ToroImplementationException(
                    "Resource '" + resourcePath + "' does not exist"
            );
        }
        Statement st = null;
        try {
            String methodAsString
                    = CharStreams.toString(
                            new BufferedReader(
                                    new InputStreamReader(
                                            resourceAsStream,
                                            Charset.forName("UTF-8")
                                    )
                            )
                    );
            st = conn.createStatement();
            st.execute(methodAsString);
        } finally {
            if (st != null) {
                st.close();
            }
            resourceAsStream.close();
        }
    }

    private void createCast(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        Statement st = null;
        try {
            st = conn.createStatement();
            
            LOGGER.debug("Removing previous varchar to jsonb cast");
            Savepoint savepoint = conn.setSavepoint();
            try {
                st.executeUpdate("DROP CAST (varchar AS jsonb)");
            } catch (SQLException ex) {
                LOGGER.debug("Old varchar to jsonb cast does not exist");
                conn.rollback(savepoint);
            }
            LOGGER.debug("Creating new varchar to jsonb cast");
            executeSql(conn, "/sql/json_cast.sql");
            LOGGER.debug("Cast varchar to jsonb cast created");
            
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

}
