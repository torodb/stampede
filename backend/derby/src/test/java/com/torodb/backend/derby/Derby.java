package com.torodb.backend.derby;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.torodb.backend.DbBackend;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.meta.TorodbSchema;

public class Derby {

	public static DbBackend getDbBackend(){
	       OfficialDerbyDriver derbyDriver = new OfficialDerbyDriver();
	       DerbyDbBackendConfiguration derbyConfiguration = new DerbyDbBackendConfiguration() {
               @Override
               public String getUsername() {
                   return null;
               }
               
               @Override
               public int getReservedReadPoolSize() {
                   return 0;
               }
               
               @Override
               public String getPassword() {
                   return null;
               }
               
               @Override
               public int getDbPort() {
                   return 0;
               }
               
               @Override
               public String getDbName() {
                   return "torodb";
               }
               
               @Override
               public String getDbHost() {
                   return null;
               }
               
               @Override
               public long getCursorTimeout() {
                   return 0;
               }
               
               @Override
               public long getConnectionPoolTimeout() {
                   return 0;
               }
               
               @Override
               public int getConnectionPoolSize() {
                   return 0;
               }

               @Override
               public boolean inMemory() {
                   return false;
               }

               @Override
               public boolean embedded() {
                   return true;
               }
           };
           
	       return new DerbyDbBackend(derbyConfiguration, derbyDriver);
	}
	
	public static void cleanDatabase(SqlInterface databaseInterface, DbBackend dbBackend) throws SQLException{
		try (Connection connection = dbBackend.getSessionDataSource().getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables("%", "%", "%", null);
            while (tables.next()) {
                String schemaName = tables.getString("TABLE_SCHEM");
                String tableName = tables.getString("TABLE_NAME");
                if (!databaseInterface.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            ResultSet schemas = metaData.getSchemas();
            while (schemas.next()) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                if (!databaseInterface.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP SCHEMA \"" + schemaName + "\" RESTRICT")) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            connection.commit();
        }
	}
}
