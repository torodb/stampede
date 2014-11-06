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

import com.torodb.di.MongoServerModule;
import com.torodb.di.DbWrapperModule;
import com.torodb.di.ConfigModule;
import com.torodb.di.DbMetaInformationCacheModule;
import com.torodb.di.ConnectionModule;
import com.torodb.di.ExecutorModule;
import com.torodb.di.D2RModule;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.beust.jcommander.JCommander;
import com.eightkdata.mongowp.mongoserver.MongoServer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.torod.core.Torod;
import java.io.*;
import java.nio.charset.Charset;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Main {
    
    private static final String VERSION = "0.12";

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
			   
			   if ((toroPassChunks[0].equals("*") || toroPassChunks[0].equals(config.getDbhost())) &&
					   (toroPassChunks[1].equals("*") || toroPassChunks[1].equals(String.valueOf(config.getDbport()))) &&
					   (toroPassChunks[2].equals("*") || toroPassChunks[2].equals(config.getDbname())) &&
					   (toroPassChunks[2].equals("*") || toroPassChunks[3].equals(config.getDbuser()))) {
				   config.setPassword(toroPassChunks[4]);
			   }
			}
			br.close();
		}

		if (!config.hasPassword() || config.askForPassword()) {
			config.setPassword(readPwd("PostgreSQL's database user password:"));
		}
		
		if (config.debug()) {
			Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
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

		config.initialize();

		Injector injector = Guice.createInjector(new ConfigModule(config),
				new MongoServerModule(), new DbWrapperModule(),
				new ExecutorModule(), new DbMetaInformationCacheModule(),
				new D2RModule(), new ConnectionModule());

		final Torod torod = injector.getInstance(Torod.class);
		final MongoServer server = injector.getInstance(MongoServer.class);

		Thread shutdown = new Thread() {
			@Override
			public void run() {
				shutdown(config, torod, server);
			}
		};
		
		Runtime.getRuntime().addShutdownHook(shutdown);

        Thread serverThread = new Thread() {
            @Override
            public void run() {
                JCommander.getConsole().println("Starting ToroDB v" + VERSION 
                        + " listening on port " + config.getPort());
                Main.run(torod, server);
                shutdown(config, torod, server);
            }
        };
        serverThread.start();
	}

	private static void run(final Torod torod, final MongoServer server) {
		torod.start();
		server.run();
	}

	private static void shutdown(final Config config,
			final Torod torod, final MongoServer server) {
		server.stop();
		torod.shutdown();
		config.shutdown();
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
