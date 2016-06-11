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

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Console;
import com.eightkdata.mongowp.server.wp.NettyMongoServer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.torodb.Shutdowner;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.mysql.MySQL;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.model.protocol.mongo.Replication;
import com.torodb.config.util.ConfigUtils;
import com.torodb.di.BackendModule;
import com.torodb.di.ConfigModule;
import com.torodb.di.ConnectionModule;
import com.torodb.di.D2RModule;
import com.torodb.di.DbMetaInformationCacheModule;
import com.torodb.di.ExecutorModule;
import com.torodb.di.ExecutorServiceModule;
import com.torodb.di.MongoConfigModule;
import com.torodb.di.MongoLayerModule;
import com.torodb.torod.core.Torod;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.util.LogbackUtils;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;

public class ToroRunnerClassRule implements TestRule {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ToroRunnerClassRule.class);

	private static final int TORO_BOOT_MAX_INTERVAL_MILLIS = 2 * 60 * 1000;
	
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
	private Shutdowner shutdowner;
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
			
			Logger root = LogbackUtils.getRootLogger();
	
			LogbackUtils.setLoggerLevel(root, config.getGeneric().getLogLevel());
			
			Appender<ILoggingEvent> uncaughtExceptionAppender = new AppenderBase<ILoggingEvent>() {
				@Override
				protected void append(ILoggingEvent eventObject) {
					IThrowableProxy throwableProxy = eventObject.getThrowableProxy();
					if (throwableProxy != null &&
							throwableProxy instanceof ThrowableProxy) {
						addUncaughtException(((ThrowableProxy) throwableProxy).getThrowable());
					}
					
				}
			};
			uncaughtExceptionAppender.setContext(LogbackUtils.getLoggerContext());
			uncaughtExceptionAppender.start();
			root.addAppender(uncaughtExceptionAppender);
			
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
			
            if (config.getBackend().isMySQLLike()) {
                MySQL mysqlBackend = config.getBackend().asMySQL();

                MysqlDataSource dataSource = new MysqlDataSource();
        
                dataSource.setUser(mysqlBackend.getUser());
                dataSource.setPassword(mysqlBackend.getPassword());
                dataSource.setServerName(mysqlBackend.getHost());
                dataSource.setPortNumber(mysqlBackend.getPort());
                dataSource.setDatabaseName("mysql");
        
                Connection connection = dataSource.getConnection();
                List<String> systemDatabases = Arrays.asList(new String[] {
                        "mysql", "information_schema", "sys", "performance_schema"
                });
                ResultSet rs = connection.prepareStatement("SELECT schema_name FROM information_schema.schemata").executeQuery();
                while (rs.next()) {
                    String database = rs.getString(1);
                    
                    if (systemDatabases.contains(database)) {
                        continue;
                    }
                    
                    try {
                        connection.prepareCall("DROP DATABASE `" + database + "`").execute();
                    } catch(SQLException psqlException) {
                    }
                }
                    
                connection.prepareCall("CREATE DATABASE "
                        + mysqlBackend.getDatabase()
                ).execute();
                connection.close();
            }
            
			Injector injector = Guice.createInjector(
					new ConfigModule(config),
					new BackendModule(config),
					new ConfigModule(config),
					new MongoConfigModule(config),
					new MongoLayerModule(config),
					new ExecutorModule(1000, 1000, 0.2),
					new DbMetaInformationCacheModule(),
					new D2RModule(),
					new ConnectionModule(),
					new ExecutorServiceModule()
			);

            final CyclicBarrier barrier = new CyclicBarrier(2);
			final Torod torod = injector.getInstance(Torod.class);
			final NettyMongoServer server = injector.getInstance(NettyMongoServer.class);
			final ReplCoordinator replCoord = injector.getInstance(ReplCoordinator.class);
			shutdowner = injector.getInstance(Shutdowner.class);
	
			Thread serverThread = new Thread() {
				@Override
				public void run() {
                    try {
                        torod.start();
                        replCoord.startAsync();
                        replCoord.awaitRunning();
                        server.startAsync().awaitRunning();
                        barrier.await();
                    } catch (Throwable e) {
                        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                }
            };
            serverThread.start();
	
			long start = System.currentTimeMillis();
            barrier.await();
			if (System.currentTimeMillis() - start >= TORO_BOOT_MAX_INTERVAL_MILLIS) {
				throw new RuntimeException(
						"Toro failed to start after waiting for " + TORO_BOOT_MAX_INTERVAL_MILLIS + " milliseconds.");
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
            case Mongo:
                if (config.getProtocol().getMongo().getReplication() != null) {
                    config.getProtocol().getMongo().setReplication(null);
                }
                break;
            case MongoReplSet:
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
            case Postgres:
                if (!config.getBackend().isPostgres()) {
                    config.getBackend().setBackendImplementation(new Postgres());
                }
                break;
            case Greenplum:
                if (!config.getBackend().isGreenplum()) {
                    config.getBackend().setBackendImplementation(new Greenplum());
                    config.getBackend().asGreenplum().setPort(6432);
                }
                break;
            case MySQL:
                if (!config.getBackend().isMySQL()) {
                    config.getBackend().setBackendImplementation(new MySQL());
                    config.getBackend().asMySQL().setPort(3307);
                }
                break;
        }
		
		try {
		    ConfigUtils.parseToropassFile(config);
            if (config.getBackend().isPostgresLike() && 
                    config.getBackend().asPostgres().getPassword() == null) {
                config.getBackend().asPostgres().setPassword("torodb");
            }
            if (config.getBackend().isMySQLLike() && 
                    config.getBackend().asMySQL().getPassword() == null) {
                config.getBackend().asMySQL().setPassword("torodb");
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
		//TODO: Fix shutdown to permit restart inside same JVM
		//shutdowner.shutdown();
		//started = false;
		
		List<Throwable> exceptions = getUcaughtExceptions();
		if (!exceptions.isEmpty()) {
			throw new RuntimeException(exceptions.get(0));
		}
	}

}
