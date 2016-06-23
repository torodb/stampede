package com.torodb.backend.postgresql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.torodb.backend.DbBackend;
import com.torodb.backend.DslContextFactory;
import com.torodb.backend.DslContextFactoryImpl;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.SqlInterfaceDelegate;
import com.torodb.backend.driver.postgresql.PostgreSQLDbBackendConfiguration;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.postgresql.guice.PostgreSQLBackendModule;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.guice.CoreModule;

public class Postgresql {

	public static Injector createInjector() {
	       return Guice.createInjector(
                   new CoreModule(),
                   new Module() {
                       @Override
                       public void configure(Binder binder) {
                           binder.bind(SqlInterface.class)
                               .to(SqlInterfaceDelegate.class)
                               .in(Singleton.class);
                           binder.bind(DslContextFactory.class)
                               .to(DslContextFactoryImpl.class)
                               .asEagerSingleton();
                       }
                   },
                    new PostgreSQLBackendModule(),
                    Postgresql.getConfigurationModule());
    }
	
	public static Module getConfigurationModule() {
	    return new Module() {
            @Override
            public void configure(Binder binder) {
                binder.bind(PostgreSQLDbBackendConfiguration.class)
                    .toInstance(getBackEndConfiguration());
            }
        };
	}
	
	public static PostgreSQLDbBackendConfiguration getBackEndConfiguration(){
		return new PostgreSQLDbBackendConfiguration() {
			@Override
			public String getUsername() {
				return "torodb";
			}

			@Override
			public int getReservedReadPoolSize() {
				return 4;
			}

			@Override
			public String getPassword() {
				return "torodb";
			}

			@Override
			public int getDbPort() {
				return 5432;
			}

			@Override
			public String getDbName() {
				return "torodb";
			}

			@Override
			public String getDbHost() {
				return "localhost";
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

		};
	}
	

	public static void cleanDatabase(Injector injector) throws SQLException {
	    DbBackend dbBackend = injector.getInstance(DbBackend.class);
	    IdentifierConstraints identifierConstraints = injector.getInstance(IdentifierConstraints.class);
	    try (Connection connection = dbBackend.createSystemConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables("%", "%", "%", new String[]{"TABLE"});
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
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP SCHEMA \"" + schemaName + "\" CASCADE ")) {
                    	System.out.println(schemaName);
                        preparedStatement.executeUpdate();
                    }
                }
            }
            connection.commit();
        }
	}
}
