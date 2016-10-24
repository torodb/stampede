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

package com.torodb.integration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.postgresql.ds.PGSimpleDataSource;

import com.beust.jcommander.internal.Console;
import com.google.inject.Injector;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.util.Log4jUtils;
import com.torodb.standalone.ToroDbServer;
import com.torodb.standalone.config.model.Config;
import com.torodb.standalone.config.model.backend.derby.Derby;
import com.torodb.standalone.config.model.backend.postgres.Postgres;

public abstract class AbstractBackendRunnerClassRule implements TestRule {

    private static final Logger LOGGER = LogManager.getLogger(AbstractBackendRunnerClassRule.class);
    private static final int TORO_BOOT_MAX_INTERVAL_MILLIS = 2 * 60 * 1000;

	@Override
	public Statement apply(final Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				try {
					startupBackend();
					
					base.evaluate();
				} finally {
					shutdownBackend();
				}
			}
		};
	}
	
	private boolean started = false;
	private Config config;
	private Injector injector;

	public AbstractBackendRunnerClassRule() {
        super();
        Config config = new Config();
        
        String yamlString = System.getProperty("torodbIntegrationConfigYml");
        
        if (yamlString != null && !yamlString.isEmpty()) {
            LOGGER.info("Reading configuration from property torodbIntegrationConfigYml:\n" + yamlString);
            
            try {
                config = ConfigUtils.readConfigFromYaml(Config.class, yamlString);
            } catch(Throwable throwable) {
                LOGGER.error("An error occurred while loading config from property torodbIntegrationConfigYml."
                        + " Check it in your ~/.m2/settings.xml", throwable);
                throw new RuntimeException("An error occurred while loading config from property torodbIntegrationConfigYml."
                        + " Check it in your ~/.m2/settings.xml", throwable);
            }
        }
        
        this.config = config;
    }

	public Config getConfig() {
		return config;
	}
	
	public Injector getInjector() {
	    return injector;
	}
	
	protected synchronized void startupBackend() throws Exception {
		if (!started) {
			setupConfig();
			
            injector = ToroDbServer.createInjector(config, Clock.systemUTC());

			Log4jUtils.setRootLevel(config.getGeneric().getLogLevel());
			
            if (config.getBackend().isLike(Postgres.class)) {
                Postgres postgresBackend = config.getBackend().as(Postgres.class);

                PGSimpleDataSource dataSource = new PGSimpleDataSource();
        
                dataSource.setUser(postgresBackend.getUser());
                dataSource.setPassword(postgresBackend.getPassword());
                dataSource.setServerName(postgresBackend.getHost());
                dataSource.setPortNumber(postgresBackend.getPort());
                dataSource.setDatabaseName("template1");
        
                Connection connection = dataSource.getConnection();
                try {
                    connection.prepareCall("DROP DATABASE " + postgresBackend.getDatabase()).execute();
                } catch(SQLException psqlException) {
                    
                }
                connection.prepareCall("CREATE DATABASE "
                        + postgresBackend.getDatabase()
                        + " OWNER " + postgresBackend.getUser()
                ).execute();
                connection.close();
            }
			
            final CyclicBarrier barrier = new CyclicBarrier(2);
	
			Thread serverThread = new Thread() {
				@Override
				public void run() {
                    try {
                        startUp();

                        barrier.await();
                    } catch (Throwable e) {
                        LogManager.getRootLogger().error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                }
            };
            serverThread.start();

            try {
                barrier.await(TORO_BOOT_MAX_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                throw new RuntimeException("Toro failed to start after waiting for " + TORO_BOOT_MAX_INTERVAL_MILLIS + " milliseconds.", ex);
            }
            
            started = true;
		}
	}
	
	protected abstract void startUp() throws Exception;

	private void setupConfig() {
        IntegrationTestEnvironment ite = 
                IntegrationTestEnvironment.CURRENT_INTEGRATION_TEST_ENVIRONMENT;
        switch(ite.getProtocol()) {
            case MONGO:
                if (config.getProtocol().getMongo().getReplication() != null) {
                    config.getProtocol().getMongo().setReplication(null);
                }
                break;
            case MONGO_REPL_SET:
                if (config.getProtocol().getMongo().getReplication() == null
                        || config.getProtocol().getMongo().getReplication().size() != 1) {
                    Replication replication = new Replication();
                    replication.setReplSetName("rs1");
                    replication.setSyncSource("localhost:27020");
                    config.getProtocol().getMongo().setReplication(
                            Arrays.asList(new Replication[]{replication}));
                }
                break;
        }

        switch(ite.getBackend()) {
            case POSTGRES:
                if (!config.getBackend().is(Postgres.class)) {
                    config.getBackend().setBackendImplementation(new Postgres());
                }
                break;
            case DERBY:
                if (!config.getBackend().is(Derby.class)) {
                    config.getBackend().setBackendImplementation(new Derby());
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
		
		try {
            if (config.getBackend().isLike(Postgres.class)) { 
                ConfigUtils.parseToropassFile(config.getBackend().as(Postgres.class));
                if (config.getBackend().as(Postgres.class).getPassword() == null) {
                    config.getBackend().as(Postgres.class).setPassword("torodb");
                }
            } else
            if (config.getBackend().isLike(Derby.class)) { 
                ConfigUtils.parseToropassFile(config.getBackend().as(Derby.class));
                if (config.getBackend().as(Derby.class).getPassword() == null) {
                    config.getBackend().as(Derby.class).setPassword("torodb");
                }
            }
		} catch(Exception exception) {
		    throw new RuntimeException(exception);
		}
		
		config.getGeneric().setLogLevel(ite.getLogLevel());
		
		ConfigUtils.validateBean(config);
		
		final StringBuilder yamlStringBuilder = new StringBuilder();
		try {
    		ConfigUtils.printYamlConfig(config, new Console() {
                @Override
                public void print(String arg0) {
                    yamlStringBuilder.append(arg0);
                }
    
                @Override
                public void println(String arg0) {
                    yamlStringBuilder.append(arg0);
                    yamlStringBuilder.append("\n");
                }
    
                @Override
                public char[] readPassword(boolean arg0) {
                    return null;
                }
            });
		} catch(Exception exception) {
		    throw new RuntimeException(exception);
		}
		
		LOGGER.info("Configuration for integration tests will be:\n" + yamlStringBuilder.toString());
	}

	private synchronized void shutdownBackend() {
		started = false;
		
        try {
            shutDown();
        } catch (Throwable e) {
            LogManager.getRootLogger().error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
	}
	
	protected abstract void shutDown() throws Exception;

    protected void cleanDatabase() throws Exception {
        DbBackendService dbBackend = injector.getInstance(DbBackendService.class);
        IdentifierConstraints identifierConstraints = injector.getInstance(IdentifierConstraints.class);
        try (Connection connection = dbBackend.createSystemConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables("%", "%", "%", new String[] { "TABLE", "VIEW" });
            List<String[]> nextToDropSchemaTableList = new ArrayList<>();
            while (tables.next()) {
                String schemaName = tables.getString("TABLE_SCHEM");
                String tableName = tables.getString("TABLE_NAME");
                nextToDropSchemaTableList.add(new String[] { schemaName, tableName });
            }
            while (!nextToDropSchemaTableList.isEmpty()) {
                List<String[]> toDropSchemaTableList = new ArrayList<>(nextToDropSchemaTableList);
                nextToDropSchemaTableList.clear();
                for (String[] toDropSchameTable : toDropSchemaTableList) {
                    String schemaName = toDropSchameTable[0];
                    String tableName = toDropSchameTable[1];
                    if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.IDENTIFIER)) {
                        try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
                            preparedStatement.executeUpdate();
                            connection.commit();
                        } catch(SQLException sqlException) {
                            connection.rollback();
                            nextToDropSchemaTableList.add(new String[] { schemaName, tableName });
                        }
                    }
                }
            }

            ResultSet schemas = metaData.getSchemas();
            while (schemas.next()) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.IDENTIFIER)) {
                    String dropSchemaStatement = "DROP SCHEMA \"" + schemaName + "\" CASCADE";
                    if (config.getBackend().isLike(Derby.class)) {
                        dropSchemaStatement = "DROP SCHEMA \"" + schemaName + "\" RESTRICT";
                    }
                    try (PreparedStatement preparedStatement = connection.prepareStatement(dropSchemaStatement)) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            connection.commit();
        }
    }
}
