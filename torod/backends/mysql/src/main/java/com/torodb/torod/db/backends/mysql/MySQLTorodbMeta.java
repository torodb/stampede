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

package com.torodb.torod.db.backends.mysql;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.inject.Provider;

import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;
import com.google.common.io.CharStreams;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.exceptions.InvalidCollectionSchemaException;
import com.torodb.torod.db.backends.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.backends.meta.CollectionSchema;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.meta.TorodbSchema;
import com.torodb.torod.db.backends.tables.AbstractCollectionsTable;
import com.torodb.torod.db.backends.tables.records.AbstractCollectionsRecord;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public class MySQLTorodbMeta implements TorodbMeta {

    private final String databaseName;
    private final ConcurrentMap<String, CollectionSchema> collectionSchemes;
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLTorodbMeta.class);
    private final DatabaseInterface databaseInterface;
    private final  Provider<SubDocType.Builder> subDocTypeBuilderProvider;

    MySQLTorodbMeta(
            String databaseName,
            DSLContext dsl,
            DatabaseInterface databaseInterface,
            @Nonnull Provider<SubDocType.Builder> subDocTypeBuilderProvider)
    throws SQLException, IOException, InvalidDatabaseException {
        this.databaseName = databaseName;
        this.databaseInterface = databaseInterface;
        this.subDocTypeBuilderProvider = subDocTypeBuilderProvider;

        Meta jooqMeta = dsl.meta();
        Connection conn = dsl.configuration().connectionProvider().acquire();
        DatabaseMetaData jdbcMeta = conn.getMetaData();

        //only system executor thread can update this map
        collectionSchemes = new MapMaker().concurrencyLevel(1).makeMap();

        TorodbSchema.TORODB.checkOrCreate(dsl, jooqMeta, jdbcMeta, databaseInterface);
        loadAllCollectionSchemas(dsl, jooqMeta, jdbcMeta);
        
        createTables(conn, jdbcMeta);
        createProcedures(conn, jdbcMeta);

        dsl.configuration().connectionProvider().release(conn);
    }
    
    private void loadAllCollectionSchemas(
            DSLContext dsl,
            Meta jooqMeta,
            DatabaseMetaData jdbcMeta) throws InvalidCollectionSchemaException {
        
        Result<AbstractCollectionsRecord> records
                = dsl.selectFrom(databaseInterface.getCollectionsTable()).fetch();
        
        for (AbstractCollectionsRecord colRecord : records) {
            CollectionSchema colSchema = new CollectionSchema(
                    colRecord.getSchema(), 
                    colRecord.getName(),
                    dsl, 
                    jdbcMeta, 
                    jooqMeta, 
                    this,
                    databaseInterface,
                    subDocTypeBuilderProvider
            );
            collectionSchemes.put(colSchema.getCollection(), colSchema);
        }
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public boolean exists(String collection) {
        return collectionSchemes.containsKey(collection);
    }

    @Override
    public CollectionSchema getCollectionSchema(String collection) {
        CollectionSchema schema = collectionSchemes.get(collection);
        if (schema == null) {
            throw new IllegalArgumentException("There is no schema associated with collection "
                    + collection);
        }
        return schema;
    }

    @Override
    public void dropCollectionSchema(String collection) {
        CollectionSchema removed = collectionSchemes.remove(collection);
        if (removed == null) {
            throw new IllegalArgumentException("Collection " + collection
                    + " didn't exist");
        }
    }

    @Override
    public CollectionSchema createCollectionSchema(
            String colName,
            String schemaName,
            DSLContext dsl) throws InvalidCollectionSchemaException {
        if (collectionSchemes.containsKey(colName)) {
            throw new IllegalArgumentException("Collection '" + colName
                    + "' is already associated with a collection schema");
        }
        CollectionSchema result = new CollectionSchema(
                schemaName, colName, dsl, this, databaseInterface, subDocTypeBuilderProvider
        );
        collectionSchemes.put(colName, result);

        return result;
    }

    @Override
    public Collection<CollectionSchema> getCollectionSchemes() {
        return Collections.unmodifiableCollection(collectionSchemes.values());
    }

    
    private void createTables(
            Connection conn,
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createSequenceTable(conn, jdbcMeta);
        createCartesianView(conn, jdbcMeta);
    }

    private void createProcedures(
            Connection conn,
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createCreateSequenceProcedure(conn, jdbcMeta);
        createNextvalFunction(conn, jdbcMeta);
        createFindDocQueryFunction(conn, jdbcMeta);
        createFindDocProcedure(conn, jdbcMeta);
        createFindDocsProcedure(conn, jdbcMeta);
        createFirstFreeDocIdProcedure(conn, jdbcMeta);
        createReserveDocIdsProcedure(conn, jdbcMeta);
        createVarcharToJsonProcedure(conn, jdbcMeta);
    }
    
    private void createSequenceTable(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createTable(conn, jdbcMeta, "sequence");
    }
    
    private void createCartesianView(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createView(conn, jdbcMeta, "cartesian");
    }
    
    private void createCreateSequenceProcedure(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "create_sequence");
    }
    
    private void createNextvalFunction(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "nextval");
    }
    
    private void createFindDocQueryFunction(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "find_doc_query");
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
    
    private void createVarcharToJsonProcedure(
            Connection conn, 
            DatabaseMetaData jdbcMeta
    ) throws SQLException, IOException {
        createProcedure(conn, jdbcMeta, "varchar_to_json");
    }
    
    private void createTable(
            Connection conn,
            DatabaseMetaData jdbcMeta,
            String table
    ) throws SQLException, IOException {
        if (!checkIfTableExists(jdbcMeta, table)) {
            LOGGER.debug("Creating table "+table);
            executeSql(conn, "/sql/mysql/"+table+"_table.sql");
            LOGGER.debug("Table "+table+" created");
        }
        else {
            LOGGER.debug("Table "+table+" found");
        }
        
    }
    
    private void createView(
            Connection conn,
            DatabaseMetaData jdbcMeta,
            String view
    ) throws SQLException, IOException {
        if (!checkIfTableExists(jdbcMeta, view)) {
            LOGGER.debug("Creating table "+view);
            executeSql(conn, "/sql/mysql/"+view+"_view.sql");
            LOGGER.debug("Table "+view+" created");
        }
        else {
            LOGGER.debug("Table "+view+" found");
        }
        
    }
    
    private void createProcedure(
            Connection conn,
            DatabaseMetaData jdbcMeta,
            String proc
    ) throws SQLException, IOException {
        if (!checkIfProcedureExists(jdbcMeta, proc)) {
            LOGGER.debug("Creating procedure "+proc);
            executeSql(conn, "/sql/mysql/"+proc+".sql");
            LOGGER.debug("Procedure "+proc+" created");
        }
        else {
            LOGGER.debug("Procedure "+proc+" found");
        }
        
    }
    
    private boolean checkIfTableExists(
            DatabaseMetaData jdbcMeta, 
            String tableName
    ) throws SQLException {
        ResultSet tables = null;
        try {
            tables = jdbcMeta.getTables("%", TorodbSchema.TORODB_SCHEMA, tableName, null);
            
            return tables.next();
        } finally {
            if (tables != null) {
                tables.close();
            }
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
                = MySQLTorodbMeta.class.getResourceAsStream(resourcePath);
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
            for (String subMethodAsString : splitMethod(methodAsString)) {
                st = conn.createStatement();
                st.execute(subMethodAsString);
            }
        } finally {
            if (st != null) {
                st.close();
            }
            resourceAsStream.close();
        }
    }
    
    private String[] splitMethod(String methodAsString) {
        List<String> subMethodsAsString = new ArrayList<String>();
        
        String splitPatternString = "^\\s*DELIMITER\\s+([^ ]+)\\s*$";
        
        Pattern splitPattern = Pattern.compile(splitPatternString, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.UNIX_LINES);
        Matcher matcher = splitPattern.matcher(methodAsString);
        
        int previousMatcherEnd = 0;
        int currentMatcherStart = 0;
        String previousDelimiter = null;
        if (matcher.find()) {
            do {
                currentMatcherStart = matcher.start();
                String subMethodAsString = methodAsString.substring(previousMatcherEnd, currentMatcherStart);
                if (previousDelimiter != null) {
                    subMethodsAsString.addAll(
                            Arrays.asList(subMethodAsString.split(Pattern.quote(previousDelimiter)))
                    );
                } else {
                    subMethodsAsString.add(subMethodAsString);
                }
                previousMatcherEnd = matcher.end();
                previousDelimiter = matcher.group(1).replace("\n", "");
            } while(matcher.find());
            currentMatcherStart = methodAsString.length();
            String subMethodAsString = methodAsString.substring(previousMatcherEnd, currentMatcherStart);
            subMethodsAsString.addAll(
                    Arrays.asList(subMethodAsString.split(Pattern.quote(previousDelimiter)))
            );
        } else {
            subMethodsAsString.add(methodAsString);
        }
        
        for (int index=0;index < subMethodsAsString.size();index++) {
            if (subMethodsAsString.get(index).matches("^\\s*$")) {
                subMethodsAsString.remove(index);
                index--;
            }
        }
        
        return subMethodsAsString.toArray(new String[subMethodsAsString.size()]);
    }

}
