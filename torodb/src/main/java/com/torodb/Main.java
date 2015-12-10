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
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.eightkdata.mongowp.mongoserver.MongoServer;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.util.ConfigUtils;
import com.torodb.di.BackendModule;
import com.torodb.di.ConfigModule;
import com.torodb.di.ConnectionModule;
import com.torodb.di.D2RModule;
import com.torodb.di.DbMetaInformationCacheModule;
import com.torodb.di.DbWrapperModule;
import com.torodb.di.ExecutorModule;
import com.torodb.di.ExecutorServiceModule;
import com.torodb.di.MongoLayerModule;
import com.torodb.torod.backend.db.postgresql.di.PostgreSQLModule;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.exceptions.TorodStartupException;
import com.torodb.torod.mongodb.repl.ReplCoordinator;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;

/**
 * ToroDB's entry point
 */
public class Main {
	public static void main(String[] args) throws Throwable {
		final CliConfig cliConfig = new CliConfig();
		JCommander jCommander = new JCommander(cliConfig, args);
		jCommander.setColumnSize(Integer.MAX_VALUE);
		
		if (cliConfig.isHelp()) {
			ResourceBundle cliMessages = PropertyResourceBundle.getBundle("CliMessages");
			ResourceBundle bundle = ConfigUtils.extractParamDescriptionFromConfigSchema(cliMessages);
			JCommander jCommanderForHelp = new JCommander(new CliConfig(), bundle);
			jCommanderForHelp.setColumnSize(Integer.MAX_VALUE);
			jCommanderForHelp.usage();
			System.exit(0);
		}
		
		final Config config = ConfigUtils.readConfig(cliConfig);
		
		if (cliConfig.isPrintConfig()) {
			ObjectMapper objectMapper = new YAMLMapper();
			objectMapper.configure(Feature.ALLOW_COMMENTS, true);
			objectMapper.configure(Feature.ALLOW_YAML_COMMENTS, true);
			objectMapper.writeValue(System.out, config);
			
			System.exit(0);
		}
		
		if (cliConfig.isPrintXmlConfig()) {
			ObjectMapper objectMapper = new XmlMapper();
			objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
			objectMapper.configure(Feature.ALLOW_COMMENTS, true);
			objectMapper.configure(Feature.ALLOW_YAML_COMMENTS, true);
			objectMapper.writeValue(System.out, config);
			
			System.exit(0);
		}
		
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		switch(config.getGeneric().getLogLevel()) {
		case NONE:
			root.setLevel(Level.OFF);
			break;
		case INFO:
			root.setLevel(Level.INFO);
			break;
		case ERROR:
			root.setLevel(Level.ERROR);
			break;
		case WARNING:
			root.setLevel(Level.WARN);
			break;
		case DEBUG:
			root.setLevel(Level.DEBUG);
			break;
		case TRACE:
			root.setLevel(Level.ALL);
			break;
		}
		
		if (config.getGeneric().getLogFile() != null) {
			FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
			fileAppender.setFile(config.getGeneric().getLogFile());
			root.addAppender(fileAppender);
		}
		
		if (config.getBackend().isPostgresLike()) {
			Postgres postgres = config.getBackend().asPostgres();

			File toroPass = new File(postgres.getToropassFile());
			if (toroPass.exists() && toroPass.canRead() && toroPass.isFile()) {
				BufferedReader br = new BufferedReader(
						new InputStreamReader(new FileInputStream(toroPass), Charset.forName("UTF-8")));
				String line;
				int index = 0;
				while ((line = br.readLine()) != null) {
					index++;
					String[] toroPassChunks = line.split(":");
					if (toroPassChunks.length != 5) {
						System.err.println("Wrong format at line " + index + " of file " + postgres.getToropassFile());
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
				postgres.setPassword(readPwd("Database user password:"));
			}
		}
		
		Injector injector = Guice.createInjector(
				new BackendModule(config),
				new PostgreSQLModule(),
				new ConfigModule(config),
				new MongoLayerModule(config),
				new DbWrapperModule(),
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
					JCommander.getConsole().println("Starting ToroDB v" + buildProperties.getFullVersion()
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

	private static String readPwd(String text) throws IOException {
		Console c = System.console();
		if (c == null) { // In Eclipse IDE
			System.out.print(text);
			InputStream in = System.in;
			int max = 50;
			byte[] b = new byte[max];

			int l = in.read(b);
			l--;// last character is \n
			if (l > 0) {
				byte[] e = new byte[l];
				System.arraycopy(b, 0, e, 0, l);
				return new String(e, Charset.forName("UTF-8"));
			} else {
				return null;
			}
		} else { // Outside Eclipse IDE
			return new String(c.readPassword(text));
		}
	}
}
