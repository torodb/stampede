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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;
import org.slf4j.LoggerFactory;

import com.eightkdata.mongowp.mongoserver.MongoServer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.Shutdowner;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.model.generic.LogLevel;
import com.torodb.config.model.protocol.mongo.Mongo;
import com.torodb.di.BackendModule;
import com.torodb.di.ConfigModule;
import com.torodb.di.ConnectionModule;
import com.torodb.di.D2RModule;
import com.torodb.di.DbMetaInformationCacheModule;
import com.torodb.di.ExecutorModule;
import com.torodb.di.ExecutorServiceModule;
import com.torodb.di.MongoConfigModule;
import com.torodb.di.MongoLayerModule;
import com.torodb.integration.config.Backend;
import com.torodb.integration.config.Protocol;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.exceptions.TorodStartupException;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.util.LogbackUtils;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;

public class ToroRunnerClassRule implements TestRule {

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
	
	private final Set<Throwable> UNCAUGHT_EXCEPTIONS = new HashSet<Throwable>();

	private boolean started = false;
	private Shutdowner shutdowner;
	private Config config;

	public void addUncaughtException(Throwable throwable) {
		synchronized (UNCAUGHT_EXCEPTIONS) {
			UNCAUGHT_EXCEPTIONS.add(throwable);
		};
	}

	public List<Throwable> getUcaughtExceptions() {
		List<Throwable> ucaughtExceptions;
		synchronized (UNCAUGHT_EXCEPTIONS) {
			ucaughtExceptions = new ArrayList<Throwable>(UNCAUGHT_EXCEPTIONS);
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
			
			config = new Config();
			
			switch(Protocol.CURRENT) {
			case Mongo:
				config.getProtocol().setMongo(new Mongo());
				break;
			}
			
			
			switch(Backend.CURRENT) {
			case Postgres:
				config.getBackend().setBackendImplementation(new Postgres());
				config.getBackend().asPostgres().setPassword("torodb");
				break;
			case Greenplum:
				config.getBackend().setBackendImplementation(new Greenplum());
				config.getBackend().asGreenplum().setPassword("torodb");
				break;
			}
			
			Thread.setDefaultUncaughtExceptionHandler(
					new Thread.UncaughtExceptionHandler() {
						@Override
						public void uncaughtException(Thread t, Throwable e) {
							addUncaughtException(e);
						}
					});
			
			new File("/tmp/data/db").mkdirs();
			
			if (config.getBackend().isPostgresLike()) {
				PGSimpleDataSource dataSource = new PGSimpleDataSource();
		
				dataSource.setUser(config.getBackend().asPostgres().getUser());
				dataSource.setPassword(config.getBackend().asPostgres().getPassword());
				dataSource.setServerName(config.getBackend().asPostgres().getHost());
				dataSource.setPortNumber(config.getBackend().asPostgres().getPort());
				dataSource.setDatabaseName("template1");
		
				Connection connection = dataSource.getConnection();
				try {
					connection.prepareCall("DROP DATABASE torod").execute();
				} catch(PSQLException psqlException) {
					
				}
				connection.prepareCall("CREATE DATABASE torod OWNER torodb").execute();
				connection.close();
			}
			
			Logger root = LogbackUtils.getRootLogger();
	
			LogbackUtils.setLoggerLevel(root, LogLevel.WARNING);
			
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
	
			final Object TORO_SEMAPHOR = new Object();
			final Torod torod = injector.getInstance(Torod.class);
			final MongoServer server = injector.getInstance(MongoServer.class);
			final ReplCoordinator replCoord = injector.getInstance(ReplCoordinator.class);
			shutdowner = injector.getInstance(Shutdowner.class);
	
			Thread serverThread = new Thread() {
				@Override
				public void run() {
					try {
						torod.start();
					} catch (TorodStartupException e) {
						LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).error(e.getMessage());
						throw new RuntimeException(e.getMessage());
					}
					replCoord.startAsync();
					replCoord.awaitRunning();
					server.run();
					synchronized (TORO_SEMAPHOR) {
						TORO_SEMAPHOR.notify();
					}
				}
			};
			serverThread.start();
	
			long start = System.currentTimeMillis();
			synchronized (TORO_SEMAPHOR) {
				TORO_SEMAPHOR.wait(TORO_BOOT_MAX_INTERVAL_MILLIS);
			}
			if (System.currentTimeMillis() - start >= TORO_BOOT_MAX_INTERVAL_MILLIS) {
				throw new RuntimeException(
						"Toro failed to start after waiting for " + TORO_BOOT_MAX_INTERVAL_MILLIS + " milliseconds.");
			}
		}
	}

	private void shutdownToro() {
		//TODO: Fix shutdown to permit restart inside same JVM
		//shutdowner.shutdown();
		//started = false;
	}

}
