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

import com.beust.jcommander.internal.Console;
import com.torodb.packaging.ToroDBServer;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.backend.postgres.Postgres;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.util.Log4jUtils;
import com.torodb.packaging.util.Log4jUtils.AppenderListener;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.postgresql.ds.PGSimpleDataSource;

public class ToroRunnerClassRule implements TestRule {

    private static final Logger LOGGER = LogManager.getLogger(ToroRunnerClassRule.class);

	private static final int TORO_BOOT_MAX_INTERVAL_MILLIS = 2 * 60 * 1000;
    private volatile ToroDBServer torodbServer;
	
	@Override
	public Statement apply(final Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				try {
					startupToro();
					
					base.evaluate();
				} finally {
					shutdownToro();
				}
			}
		};
	}
	
	private final Set<Throwable> UNCAUGHT_EXCEPTIONS = new HashSet<>();

	private boolean started = false;
	private final Config config;

	public ToroRunnerClassRule() {
        super();
        
        Config config = new Config();
        
        String yamlString = System.getProperty("torodbIntegrationConfigYml");
        
        if (yamlString != null && !yamlString.isEmpty()) {
            LOGGER.info("Reading configuration from property torodbIntegrationConfigYml:\n" + yamlString);
            
            try {
                config = ConfigUtils.readConfigFromYaml(yamlString);
            } catch(Throwable throwable) {
                LOGGER.error("An error occurred while loading config from property torodbIntegrationConfigYml."
                        + " Check it in your ~/.m2/settings.xml", throwable);
                throw new RuntimeException("An error occurred while loading config from property torodbIntegrationConfigYml."
                        + " Check it in your ~/.m2/settings.xml", throwable);
            }
        }
        
        this.config = config;
    }

    public void addUncaughtException(Throwable throwable) {
		synchronized (UNCAUGHT_EXCEPTIONS) {
			UNCAUGHT_EXCEPTIONS.add(throwable);
		};
	}

	public List<Throwable> getUcaughtExceptions() {
		List<Throwable> ucaughtExceptions;
		synchronized (UNCAUGHT_EXCEPTIONS) {
			ucaughtExceptions = new ArrayList<>(UNCAUGHT_EXCEPTIONS);
			UNCAUGHT_EXCEPTIONS.clear();
		}
		return ucaughtExceptions;
	}

	public Config getConfig() {
		return config;
	}
	
	protected void startupToro() throws Exception {
		if (!started) {
			started = true;
			
			setupConfig();
			
			Thread.setDefaultUncaughtExceptionHandler(
					new Thread.UncaughtExceptionHandler() {
						@Override
						public void uncaughtException(Thread t, Throwable e) {
							addUncaughtException(e);
						}
					});
			
			new File("/tmp/data/db").mkdirs();
			
			Log4jUtils.setRootLevel(config.getGeneric().getLogLevel());
			
			AppenderListener uncaughtExceptionAppenderListener = new AppenderListener() {
                @Override
                public void listen(String text, Throwable throwable) {
                    if (throwable != null) {
                        addUncaughtException(throwable);
                    }
                }
			};
			Log4jUtils.addRootAppenderListener(uncaughtExceptionAppenderListener);
			
            if (config.getBackend().isPostgresLike()) {
                Postgres postgresBackend = config.getBackend().asPostgres();

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
                        torodbServer = ToroDBServer.create(config, Clock.systemUTC());
                        torodbServer.startAsync();
                        torodbServer.awaitRunning();

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
            List<Throwable> exceptions = getUcaughtExceptions();
            if (!exceptions.isEmpty()) {
                throw new RuntimeException(exceptions.get(0));
            }
		}
	}

	private void setupConfig() {
        IntegrationTestEnvironment ite = IntegrationTestEnvironment.CURRENT_INTEGRATION_TEST_ENVIRONMENT;
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
                if (!config.getBackend().isPostgres()) {
                    config.getBackend().setBackendImplementation(new Postgres());
                }
                break;
            case DERBY:
                if (!config.getBackend().isDerby()) {
                    config.getBackend().setBackendImplementation(new Derby());
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
		
		try {
		    ConfigUtils.parseToropassFile(config);
            if (config.getBackend().isPostgresLike() && 
                    config.getBackend().asPostgres().getPassword() == null) {
                config.getBackend().asPostgres().setPassword("torodb");
            } else
            if (config.getBackend().isDerbyLike() && 
                    config.getBackend().asDerby().getPassword() == null) {
                config.getBackend().asDerby().setPassword("torodb");
            } else {
                throw new UnsupportedOperationException();
            }
		} catch(Exception exception) {
		    throw new RuntimeException(exception);
		}
		
		config.getGeneric().setLogLevel(IntegrationTestEnvironment.CURRENT_INTEGRATION_TEST_ENVIRONMENT.getLogLevel());
		
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
		
		LOGGER.info("Configuration for ToroDB integration tests will be:\n" + yamlStringBuilder.toString());
	}

	private void shutdownToro() {
        ToroDBServer torodbServer = this.torodbServer;
        if (torodbServer != null) {
            torodbServer.stopAsync();
            torodbServer.awaitTerminated();
        }
		started = false;
		
		List<Throwable> exceptions = getUcaughtExceptions();
		if (!exceptions.isEmpty()) {
			throw new RuntimeException(exceptions.get(0));
		}
	}

}
