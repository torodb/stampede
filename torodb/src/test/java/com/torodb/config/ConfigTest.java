package com.torodb.config;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.torodb.CliConfig;
import com.torodb.config.model.Config;
import com.torodb.config.util.ConfigUtils;

/**
 *
 */
public class ConfigTest {

	public ConfigTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	@Test
	public void testParse() throws Exception {
		ConfigUtils.readConfig(new CliConfig());
	}

	@Test
	public void testParseWithParam() throws Exception {
		final String logFile = "/tmp/torodb.log";
		
		CliConfig cliConfig = new CliConfig() {
			@Override
			public List<String> getParams() {
				String[] params = new String[] { 
					"/generic/logFile=" + logFile 
				};
				return Arrays.asList(params);
			}
		};
		Config config = ConfigUtils.readConfig(cliConfig);
		
		Assert.assertEquals("", config.getGeneric().getLogFile(), logFile);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testParseWithWrongTypeParam() throws Exception {
		CliConfig cliConfig = new CliConfig() {
			@Override
			public List<String> getParams() {
				String[] params = new String[] { 
					"/generic/logLevel=ALL" 
				};
				return Arrays.asList(params);
			}
		};
		ConfigUtils.readConfig(cliConfig);
	}

}
