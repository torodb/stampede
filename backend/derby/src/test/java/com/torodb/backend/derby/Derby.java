package com.torodb.backend.derby;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.torodb.backend.DatabaseForTest;
import com.torodb.backend.DbBackend;
import com.torodb.backend.derby.guice.DerbyBackendModule;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.core.backend.IdentifierConstraints;

public class Derby implements DatabaseForTest{

	@Override
	public List<Module> getModules() {
		return Arrays.asList(new DerbyBackendModule(),getConfigurationModule());
	}

	public static Module getConfigurationModule() {
	    return new Module() {
            @Override
            public void configure(Binder binder) {
                binder.bind(DerbyDbBackendConfiguration.class)
                    .toInstance(getBackEndConfiguration());
            }
        };
	}
	
	public static DerbyDbBackendConfiguration getBackEndConfiguration(){
		return new DerbyDbBackendConfiguration() {
            @Override
            public String getUsername() {
                return null;
            }
            
            @Override
            public int getReservedReadPoolSize() {
                return 4;
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
                return 8000;
            }
            
            @Override
            public long getConnectionPoolTimeout() {
                return 10000;
            }
            
            @Override
            public int getConnectionPoolSize() {
                return 8;
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
	}
	
	public void cleanDatabase(Injector injector) throws SQLException {
	    DbBackend dbBackend = injector.getInstance(DbBackend.class);
	    IdentifierConstraints identifierConstraints = injector.getInstance(IdentifierConstraints.class);
		try (Connection connection = dbBackend.createSystemConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables("%", "%", "%", null);
            while (tables.next()) {
                String schemaName = tables.getString("TABLE_SCHEM");
                String tableName = tables.getString("TABLE_NAME");
                if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            ResultSet schemas = metaData.getSchemas();
            while (schemas.next()) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP SCHEMA \"" + schemaName + "\" RESTRICT")) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            connection.commit();
        }
	}


}
