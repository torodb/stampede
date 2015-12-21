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

package com.torodb;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.internal.Console;
import com.eightkdata.mongowp.mongoserver.MongoServer;
import com.google.common.base.Charsets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.model.generic.LogLevel;
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
import com.torodb.torod.core.exceptions.TorodStartupException;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.util.LogbackUtils;

import ch.qos.logback.classic.Logger;

/**
 * ToroDB's entry point
 */
public class Main {
	
	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) throws Exception {
		Console console = JCommander.getConsole();

		Logger root = LogbackUtils.getRootLogger();
		
		LogbackUtils.setLoggerLevel(root, LogLevel.NONE);

		ResourceBundle cliBundle = PropertyResourceBundle.getBundle("CliMessages");
		final CliConfig cliConfig = new CliConfig();
		JCommander jCommander = new JCommander(cliConfig, cliBundle, args);
		jCommander.setColumnSize(Integer.MAX_VALUE);
		
		if (cliConfig.isHelp()) {
			jCommander.usage();
			System.exit(0);
		}

		if (cliConfig.isHelpParam()) {
			console.println(cliBundle.getString("help-param-header"));
			ConfigUtils.printParamDescriptionFromConfigSchema(console, 0);
			System.exit(0);
		}
		
		
		final Config config = ConfigUtils.readConfig(cliConfig);
		
		if (cliConfig.isPrintConfig()) {
			ConfigUtils.printYamlConfig(config, console);
			
			System.exit(0);
		}
		
		if (cliConfig.isPrintXmlConfig()) {
			ConfigUtils.printXmlConfig(config, console);
			
			System.exit(0);
		}
		
		if (config.getGeneric().getLogbackFile() != null) {
			LogbackUtils.reconfigure(config.getGeneric().getLogbackFile());
		} else {
			LogbackUtils.setLoggerLevel(root, config.getGeneric().getLogLevel());
			
			if (config.getGeneric().getLogPackages() != null) {
				LogbackUtils.setLogPackages(config.getGeneric().getLogPackages());
			}
			
			if (config.getGeneric().getLogFile() != null) {
				LogbackUtils.appendToLogFile(config.getGeneric().getLogFile());
			}
		}
		
		if (config.getBackend().isPostgresLike()) {
			Postgres postgres = config.getBackend().asPostgres();

			File toroPass = new File(postgres.getToropassFile());
			if (toroPass.exists() && toroPass.canRead() && toroPass.isFile()) {
				BufferedReader br = new BufferedReader(
						new InputStreamReader(new FileInputStream(toroPass), Charsets.UTF_8));
				String line;
				int index = 0;
				while ((line = br.readLine()) != null) {
					index++;
					String[] toroPassChunks = line.split(":");
					if (toroPassChunks.length != 5) {
						LOGGER.warn("Wrong format at line " + index + " of file " + postgres.getToropassFile());
						continue;
					}

					if ((toroPassChunks[0].equals("*") || toroPassChunks[0].equals(postgres.getHost()))
							&& (toroPassChunks[1].equals("*")
									|| toroPassChunks[1].equals(String.valueOf(postgres.getPort())))
							&& (toroPassChunks[2].equals("*") || toroPassChunks[2].equals(postgres.getDatabase()))
							&& (toroPassChunks[3].equals("*") || toroPassChunks[3].equals(postgres.getUser()))) {
						postgres.setPassword(toroPassChunks[4]);
					}
				}
				br.close();
			}

			if (cliConfig.askForPassword()) {
				console.print("Database user password:");
				postgres.setPassword(readPwd());
			}
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

		try {
			final Torod torod = injector.getInstance(Torod.class);
			final MongoServer server = injector.getInstance(MongoServer.class);
			final DefaultBuildProperties buildProperties = injector.getInstance(DefaultBuildProperties.class);
			final ReplCoordinator replCoord = injector.getInstance(ReplCoordinator.class);
			final Shutdowner shutdowner = injector.getInstance(Shutdowner.class);

			Thread shutdown = new Thread() {
				@Override
				public void run() {
					shutdowner.shutdown();
				}
			};

			Runtime.getRuntime().addShutdownHook(shutdown);

			Thread serverThread = new Thread() {
				@Override
				public void run() {
					LOGGER.info("Starting ToroDB v" + buildProperties.getFullVersion()
							+ " listening on port " + config.getBackend().asPostgres().getPort());
					Main.run(torod, server, replCoord);
				}
			};
			serverThread.start();
		} catch (ProvisionException pe) {
			String causeMessage;
			if (pe.getCause() != null) {
				causeMessage = pe.getCause().getMessage();
			} else {
				causeMessage = pe.getMessage();
			}
			JCommander.getConsole().println(causeMessage);
			System.exit(1);
		}
	}

	private static void run(final Torod torod, final MongoServer server, final ReplCoordinator replCoord) {
		try {
			torod.start();
		} catch (TorodStartupException e) {
			LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).error(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
		replCoord.startAsync();
		replCoord.awaitRunning();
		server.run();
	}

	private static String readPwd() throws IOException {
		Console c = JCommander.getConsole();
		if (System.console() == null) { // In Eclipse IDE
			InputStream in = System.in;
			int max = 50;
			byte[] b = new byte[max];

			int l = in.read(b);
			l--;// last character is \n
			if (l > 0) {
				byte[] e = new byte[l];
				System.arraycopy(b, 0, e, 0, l);
				return new String(e, Charsets.UTF_8);
			} else {
				return null;
			}
		} else { // Outside Eclipse IDE
			return new String(c.readPassword(false));
		}
	}
}
