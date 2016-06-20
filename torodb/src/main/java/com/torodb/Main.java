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


import java.io.IOException;
import java.io.InputStream;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.internal.Console;
import com.google.common.base.Charsets;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.model.generic.LogLevel;
import com.torodb.config.util.ConfigUtils;
import com.torodb.util.Log4jUtils;

/**
 * ToroDB's entry point
 */
public class Main {
	
	private static final Logger LOGGER = LogManager.getLogger(Main.class);
	
	public static void main(String[] args) throws Exception {
		Console console = JCommander.getConsole();

		Log4jUtils.setRootLevel(LogLevel.NONE);

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
			Log4jUtils.reconfigure(config.getGeneric().getLogbackFile());
		} else {
			Log4jUtils.setRootLevel(config.getGeneric().getLogLevel());
			
			if (config.getGeneric().getLogPackages() != null) {
				Log4jUtils.setLogPackages(config.getGeneric().getLogPackages());
			}
			
			if (config.getGeneric().getLogFile() != null) {
				Log4jUtils.appendToLogFile(config.getGeneric().getLogFile());
			}
		}
		
		ConfigUtils.parseToropassFile(config);
		
        if (config.getBackend().isPostgresLike()) {
            Postgres postgres = config.getBackend().asPostgres();

			if (cliConfig.isAskForPassword()) {
				console.print("Database user password:");
				postgres.setPassword(readPwd());
			}
		}
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
