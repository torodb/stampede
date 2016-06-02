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

package com.torodb.backend.derby;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;

import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.CharStreams;
import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.exceptions.InvalidDatabaseException;
import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.meta.DatabaseSchema;
import com.torodb.backend.meta.TorodbMeta;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.mocks.ToroImplementationException;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public class DerbyTorodbMeta implements TorodbMeta {

    private static final long serialVersionUID = -4785629402L;

    private static final Logger LOGGER = LoggerFactory.getLogger(DerbyTorodbMeta.class);
    private final DatabaseInterface databaseInterface;

    DerbyTorodbMeta(
            DSLContext dsl,
            DatabaseInterface databaseInterface)
    throws SQLException, IOException, InvalidDatabaseException {
        this.databaseInterface = databaseInterface;

        Meta jooqMeta = dsl.meta();
        Connection conn = dsl.configuration().connectionProvider().acquire();
        DatabaseMetaData jdbcMeta = conn.getMetaData();

        TorodbSchema.TORODB.checkOrCreate(dsl, jooqMeta, databaseInterface);
        loadAllCollectionSchemas(dsl, jooqMeta);
        
        createTypes(conn, jdbcMeta);
        createProcedures(conn, jdbcMeta);

        dsl.configuration().connectionProvider().release(conn);
    }
    
    private void loadAllCollectionSchemas(
            DSLContext dsl,
            Meta jooqMeta) throws InvalidDatabaseSchemaException {
        
        MetaDatabaseTable metaDatabaseTable = databaseInterface.getMetaDatabaseTable();
        Result<MetaDatabaseRecord> records
                = dsl.selectFrom(metaDatabaseTable).fetch();
        
        ImmutableMetaSnapshot.Builder metaSnapshotBuilder = new ImmutableMetaSnapshot.Builder();
        for (MetaDatabaseRecord colRecord : records) {
            ImmutableMetaDatabase.Builder metaDatabaseBuilder = new ImmutableMetaDatabase.Builder(
                    colRecord.getName(), colRecord.getIdentifier());
            DatabaseSchema databaseSchema = new DatabaseSchema(
                    colRecord.getName(),
                    colRecord.getIdentifier(), 
                    dsl, 
                    jooqMeta, 
                    metaDatabaseBuilder,
                    databaseInterface
            );
            
        }
    }

    private void createTypes(
            Connection conn,
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        boolean findDocTypeExists = false;
        boolean mongoObjectIdExists = false;
        boolean mongoTimestampExists = false;
        
        try (ResultSet typeInfo = jdbcMeta.getTypeInfo()) {
        
            while (typeInfo.next()) {
                findDocTypeExists = 
                        findDocTypeExists 
                        || typeInfo.getString("TYPE_NAME").equals("find_doc_type");
                mongoObjectIdExists =
                        mongoObjectIdExists
                        || typeInfo.getString("TYPE_NAME").equals("mongo_object_id");
                mongoTimestampExists =
                        mongoObjectIdExists
                        || typeInfo.getString("TYPE_NAME").equals("mongo_timestamp");
                if (findDocTypeExists && mongoObjectIdExists && mongoTimestampExists) {
                    break;
                }
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
        if (!mongoObjectIdExists) {
            LOGGER.debug("Creating type mongo_object_id");
            createMongoObjectIdType(conn);
            LOGGER.debug("Created type mongo_object_id");
        }
        else {
            LOGGER.debug("Type mongo_object_id found");
        }
        if (!mongoTimestampExists) {
            LOGGER.debug("Creating type mongo_timestamp");
            createMongoTimestampType(conn);
            LOGGER.debug("Created type mongo_timestamp");
        }
        else {
            LOGGER.debug("Type mongo_object_id found");
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
        executeSql(conn, "/sql/derby/find_doc_type.sql");
    }

    private void createMongoObjectIdType(Connection conn) throws IOException, SQLException {
        executeSql(conn, "/sql/derby/mongo_object_id_type.sql");
    }
    
    private void createMongoTimestampType(Connection conn) throws IOException, SQLException {
        executeSql(conn, "/sql/derby/mongo_timestamp_type.sql");
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
            executeSql(conn, "/sql/derby/"+proc+".sql");
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
            procedures = jdbcMeta.getProcedures("%", TorodbSchema.TORODB_SCHEMA, procedureName);
            
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
                = DerbyTorodbMeta.class.getResourceAsStream(resourcePath);
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
}
