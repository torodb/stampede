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


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import com.beust.jcommander.JCommander;
import com.eightkdata.mongowp.mongoserver.MongoServer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.torodb.di.*;
import com.torodb.torod.backend.db.postgresql.di.PostgreSQLModule;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.exceptions.TorodStartupException;

import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * ToroDB's entry point
 */
public class Main {
	public static void main(String[] args) throws Exception {
		final Config config = new Config();
		JCommander jCommander = new JCommander(config, args);
		
		if (config.help()) {
			jCommander.usage();
			System.exit(0);
		}
		
		File toroPass = new File(System.getProperty("user.home") + "/.toropass");
		if (toroPass.exists() && toroPass.canRead() && toroPass.isFile()) {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(toroPass), Charset.forName("UTF-8")));
			String line;
			while ((line = br.readLine()) != null) {
			   String[] toroPassChunks = line.split(":");
			   if (toroPassChunks.length != 5) {
				   continue;
			   }
			   
			   if ((toroPassChunks[0].equals("*") || toroPassChunks[0].equals(config.getDbHost())) &&
					   (toroPassChunks[1].equals("*") || toroPassChunks[1].equals(String.valueOf(config.getDbPort()))) &&
					   (toroPassChunks[2].equals("*") || toroPassChunks[2].equals(config.getDbName())) &&
					   (toroPassChunks[2].equals("*") || toroPassChunks[3].equals(config.getUsername()))) {
				   config.setPassword(toroPassChunks[4]);
			   }
			}
			br.close();
		}

		if (!config.hasPassword() || config.askForPassword()) {
			config.setPassword(readPwd("PostgreSQL's database user password:"));
		}
		
		if (config.debug()) {
			Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
			root.setLevel(Level.DEBUG);
		} else {
            if (config.verbose()) {
                Logger root
                        = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
                root.setLevel(Level.INFO);
            }
            else {
                Logger root
                        = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
                root.setLevel(Level.WARN);
            }
        }

		Injector injector = Guice.createInjector(
				new BackendModule(config),
				new PostgreSQLModule(),
				new ConfigModule(config),
				new MongoServerModule(),
				new DbWrapperModule(),
				new ExecutorModule(1000, 1000, 0.2),
				new DbMetaInformationCacheModule(),
				new D2RModule(),
				new ConnectionModule(),
				new InnerCursorManagerModule()
		);

        final DbBackend dbBackend;
        try {
            dbBackend = injector.getInstance(DbBackend.class);
            final Torod torod = injector.getInstance(Torod.class);
            final MongoServer server = injector.getInstance(MongoServer.class);
            final BuildProperties buildProperties
                    = injector.getInstance(BuildProperties.class);

            Thread shutdown = new Thread() {
                @Override
                public void run() {
                    shutdown(dbBackend, torod, server);
                }
            };

            Runtime.getRuntime().addShutdownHook(shutdown);

            Thread serverThread = new Thread() {
                @Override
                public void run() {
                    JCommander.getConsole().println(
                            "Starting ToroDB v"
                            + buildProperties.getFullVersion()
                            + " listening on port " + config.getPort()
                    );
                    Main.run(torod, server);
                    shutdown(dbBackend, torod, server);
                }
            };
            serverThread.start();
        }
        catch (ProvisionException pe) {
            String causeMessage;
            if (pe.getCause() != null) {
                causeMessage = pe.getCause().getMessage();
            }
            else {
                causeMessage = pe.getMessage();
            }
            JCommander.getConsole().println(causeMessage);
            System.exit(1);
        }
    }

	private static void run(final Torod torod, final MongoServer server) {
		try {
			torod.start();
		} catch (TorodStartupException e) {
			LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).error(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
		server.run();
	}

	private static void shutdown(final DbBackend dbBackend, final Torod torod, final MongoServer server) {
		server.stop();
		torod.shutdown();
		dbBackend.shutdown();
	}

	private static String readPwd(String text) throws IOException {
		Console c = System.console();
		if (c == null) { // IN ECLIPSE IDE
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
