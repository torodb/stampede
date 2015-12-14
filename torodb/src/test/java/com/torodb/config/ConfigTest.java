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

package com.torodb.config;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.torodb.CliConfig;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.model.generic.LogLevel;
import com.torodb.config.model.protocol.mongo.Role;
import com.torodb.config.util.ConfigUtils;

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
	public void testPrintConf() throws Exception {
		ByteArrayConsole byteArrayConsole = new ByteArrayConsole();
		ConfigUtils.printYamlConfig(new Config(), byteArrayConsole);
		YAMLMapper yamlMapper = new YAMLMapper();
		yamlMapper.readValue(byteArrayConsole.getByteArrayOutputStream().toByteArray(), Config.class);
	}

	@Test
	public void testPrintXmlConf() throws Exception {
		ByteArrayConsole byteArrayConsole = new ByteArrayConsole();
		ConfigUtils.printXmlConfig(new Config(), byteArrayConsole);
		XmlMapper xmlMapper = new XmlMapper();
		xmlMapper.readValue(byteArrayConsole.getByteArrayOutputStream().toByteArray(), Config.class);
	}

	@Test
	public void testHelpParam() throws Exception {
		ConfigUtils.printParamDescriptionFromConfigSchema(new ByteArrayConsole(), 0);
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
		
		Assert.assertEquals("Parameter has different value than that specified", config.getGeneric().getLogFile(), logFile);
	}

	@Test
	public void testParseWithLogPackagesParam() throws Exception {
		final String logPackage = "com.torodb";
		final LogLevel logLevel = LogLevel.NONE;
		
		CliConfig cliConfig = new CliConfig() {
			@Override
			public List<String> getParams() {
				String[] params = new String[] { 
					"/generic/logPackages/" + logPackage + "=" + logLevel.name() 
				};
				return Arrays.asList(params);
			}
		};
		Config config = ConfigUtils.readConfig(cliConfig);
		
		Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages() != null);
		Assert.assertTrue("/generic/logPackages/" + logPackage + " not defined", config.getGeneric().getLogPackages().get(logPackage) != null);
		Assert.assertEquals("/generic/logPackages has not 1 entry", config.getGeneric().getLogPackages().size(), 1);
		Assert.assertEquals("/generic/logPackages/" + logPackage + " has different value than that specified", config.getGeneric().getLogPackages().get(logPackage), logLevel);
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

	@Test
	public void testParseWithEmptyYAML() throws Exception {
		CliConfig cliConfig = new CliConfig() {
			@Override
			public boolean hasConfFile() {
				return true;
			}
			@Override
			public InputStream getConfInputStream() {
				return ConfigTest.class.getResourceAsStream("/test-parse-with-empty-yaml.yml");
			}
		};
		Config config = ConfigUtils.readConfig(cliConfig);
		
		Assert.assertTrue("/generic not defined", config.getGeneric() != null);
		Assert.assertTrue("/protocol not defined", config.getProtocol() != null);
		Assert.assertTrue("/protocol/mongo not defined", config.getProtocol().getMongo() != null);
		Assert.assertTrue("/protocol/mongo/net not defined", config.getProtocol().getMongo().getNet() != null);
		Assert.assertTrue("/backend not defined", config.getBackend() != null);
		Assert.assertEquals("/backend/postgres not defined", config.getBackend().getBackendImplementation().getClass(), Postgres.class);
		Assert.assertTrue("/backend/postgres not identified as Postgres", config.getBackend().isPostgres());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testParseWithWrongYAML() throws Exception {
		CliConfig cliConfig = new CliConfig() {
			@Override
			public boolean hasConfFile() {
				return true;
			}
			@Override
			public InputStream getConfInputStream() {
				return ConfigTest.class.getResourceAsStream("/test-parse-with-wrong-yaml.yml");
			}
		};
		ConfigUtils.readConfig(cliConfig);
	}

	@Test
	public void testParseWithYAML() throws Exception {
		CliConfig cliConfig = new CliConfig() {
			@Override
			public boolean hasConfFile() {
				return true;
			}
			@Override
			public InputStream getConfInputStream() {
				return ConfigTest.class.getResourceAsStream("/test-parse-with-yaml.yml");
			}
		};
		Config config = ConfigUtils.readConfig(cliConfig);
		
		Assert.assertTrue("/generic not defined", config.getGeneric() != null);
		Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages() != null);
		Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric().getLogPackages().get("com.torodb") != null);
		Assert.assertEquals("/generic/logLevel has different value than that specified", config.getGeneric().getLogLevel(), LogLevel.NONE);
		Assert.assertEquals("/generic/logPackages has not 1 entry", config.getGeneric().getLogPackages().size(), 1);
		Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified", config.getGeneric().getLogPackages().get("com.torodb"), LogLevel.DEBUG);
		Assert.assertTrue("/protocol not defined", config.getProtocol() != null);
		Assert.assertTrue("/protocol/mongo not defined", config.getProtocol().getMongo() != null);
		Assert.assertTrue("/protocol/mongo/net not defined", config.getProtocol().getMongo().getNet() != null);
		Assert.assertTrue("/protocol/mongo/replication not defined", config.getProtocol().getMongo().getReplication() != null);
		Assert.assertEquals("/protocol/mongo/net/port has different value than that specified", config.getProtocol().getMongo().getNet().getPort(), Integer.valueOf(27019));
		Assert.assertEquals("/protocol/mongo/replication has not 1 element", config.getProtocol().getMongo().getReplication().size(), 1);
		Assert.assertEquals("/protocol/mongo/replication/0/replSetName has different value than that specified", config.getProtocol().getMongo().getReplication().get(0).getReplSetName(), "rs1");
		Assert.assertEquals("/protocol/mongo/replication/0/role has different value than that specified", config.getProtocol().getMongo().getReplication().get(0).getRole(), Role.HIDDEN_SLAVE);
		Assert.assertEquals("/protocol/mongo/replication/0/syncSource has different value than that specified", config.getProtocol().getMongo().getReplication().get(0).getSyncSource(), "localhost:27017");
		Assert.assertTrue("/backend not defined", config.getBackend() != null);
		Assert.assertEquals("/backend/postgres not defined", config.getBackend().getBackendImplementation().getClass(), Postgres.class);
		Assert.assertTrue("/backend/postgres not identified as Postgres", config.getBackend().isPostgres());
		Assert.assertTrue("/backend/postgres not identified as Postgres Like", config.getBackend().isPostgresLike());
		Assert.assertEquals("/backend/postgres/host has different value than that specified", config.getBackend().asPostgres().getHost(), "localhost");
		Assert.assertEquals("/backend/postgres/port has different value than that specified", config.getBackend().asPostgres().getPort(), Integer.valueOf(5432));
		Assert.assertEquals("/backend/postgres/user has different value than that specified", config.getBackend().asPostgres().getUser(), "root");
		Assert.assertEquals("/backend/postgres/password has different value than that specified", config.getBackend().asPostgres().getPassword(), null);
	}

	//TODO: This particular test should pass when json schema draft v4 or greater will be used
	//actually this test does not pass becouse of missing properties
	//workaround to this behaviour by passing param /backend=null before others params
	//@Test
	public void testParseWithYAMLAndReplaceBackendWithParam() throws Exception {
		CliConfig cliConfig = new CliConfig() {
			@Override
			public boolean hasConfFile() {
				return true;
			}
			@Override
			public InputStream getConfInputStream() {
				return ConfigTest.class.getResourceAsStream("/test-parse-with-yaml.yml");
			}
			@Override
			public List<String> getParams() {
				String[] params = new String[] { 
					"/backend/greenplum/host=localhost", 
					"/backend/greenplum/port=5432", 
					"/backend/greenplum/user=root", 
					"/backend/greenplum/password=toor"
				};
				return Arrays.asList(params);
			}
		};
		Config config = ConfigUtils.readConfig(cliConfig);
		
		Assert.assertTrue("/backend not defined", config.getBackend() != null);
		Assert.assertEquals("/backend/greenplum not defined", config.getBackend().getBackendImplementation().getClass(), Greenplum.class);
		Assert.assertTrue("/backend/greenplum not identified as Greenplum", config.getBackend().isGreenplum());
		Assert.assertTrue("/backend/greenplum not identified as Postgres Like", config.getBackend().isPostgresLike());
		Assert.assertEquals("/backend/greenplum/host has different value than that specified", config.getBackend().asGreenplum().getHost(), "localhost");
		Assert.assertEquals("/backend/greenplum/port has different value than that specified", config.getBackend().asGreenplum().getPort(), Integer.valueOf(5432));
		Assert.assertEquals("/backend/greenplum/user has different value than that specified", config.getBackend().asGreenplum().getUser(), "root");
		Assert.assertEquals("/backend/greenplum/password has different value than that specified", config.getBackend().asGreenplum().getPassword(), "toor");
	}

	@Test
	public void testParseWithYAMLUsingGreenplum() throws Exception {
		CliConfig cliConfig = new CliConfig() {
			@Override
			public boolean hasConfFile() {
				return true;
			}
			@Override
			public InputStream getConfInputStream() {
				return ConfigTest.class.getResourceAsStream("/test-parse-with-yaml-using-greenplum.yml");
			}
		};
		Config config = ConfigUtils.readConfig(cliConfig);
		
		Assert.assertTrue("/backend not defined", config.getBackend() != null);
		Assert.assertEquals("/backend/greenplum not defined", config.getBackend().getBackendImplementation().getClass(), Greenplum.class);
		Assert.assertTrue("/backend/greenplum not identified as Greenplum", config.getBackend().isGreenplum());
		Assert.assertTrue("/backend/greenplum not identified as Postgres Like", config.getBackend().isPostgresLike());
		Assert.assertEquals("/backend/greenplum/host has different value than that specified", config.getBackend().asGreenplum().getHost(), "localhost");
		Assert.assertEquals("/backend/greenplum/port has different value than that specified", config.getBackend().asGreenplum().getPort(), Integer.valueOf(5432));
		Assert.assertEquals("/backend/greenplum/user has different value than that specified", config.getBackend().asGreenplum().getUser(), "root");
		Assert.assertEquals("/backend/greenplum/password has different value than that specified", config.getBackend().asGreenplum().getPassword(), null);
	}

}
