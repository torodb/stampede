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

package com.torodb.stampede.config;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.backend.postgres.Postgres;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.FilterList.IndexFilter;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.stampede.CliConfig;
import com.torodb.stampede.CliConfigUtils;
import com.torodb.stampede.config.model.Config;

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
		Config config = new Config();
		config.getReplication().setReplSetName("rs1");
		ConfigUtils.printYamlConfig(config, byteArrayConsole);
		ConfigUtils.readConfigFromYaml(Config.class, new String(byteArrayConsole.getByteArrayOutputStream().toByteArray()));
	}

	@Test
	public void testPrintXmlConf() throws Exception {
		ByteArrayConsole byteArrayConsole = new ByteArrayConsole();
        Config config = new Config();
        config.getReplication().setReplSetName("rs1");
		ConfigUtils.printXmlConfig(config, byteArrayConsole);
        ConfigUtils.readConfigFromXml(Config.class, new String(byteArrayConsole.getByteArrayOutputStream().toByteArray()));
	}

	@Test
	public void testHelpParam() throws Exception {
		ConfigUtils.printParamDescriptionFromConfigSchema(Config.class, new ByteArrayConsole(), 0);
	}

	@Test
	public void testParse() throws Exception {
	    CliConfig cliConfig = new CliConfig();
	    Field field = ImmutableList.copyOf(CliConfig.class.getDeclaredFields()).stream()
	        .filter(f -> f.getName().equals("params"))
	        .findAny().get();
	    field.setAccessible(true);
	    field.set(cliConfig, new ArrayList<>());
	    cliConfig.getParams().add("/replication/replSetName=rs1");
		CliConfigUtils.readConfig(cliConfig);
	}

    @Test
    public void testParseWithParam() throws Exception {
        File tempFile = File.createTempFile("torodb", ".log");
        tempFile.deleteOnExit();
        final String logFile = tempFile.getPath();
        
        CliConfig cliConfig = new CliConfig() {
            @Override
            public List<String> getParams() {
                String[] params = new String[] { 
                    "/generic/logFile=" + logFile,
                    "/replication/replSetName=rs1"
                };
                return Arrays.asList(params);
            }
        };
        Config config = CliConfigUtils.readConfig(cliConfig);
        
        Assert.assertEquals("Parameter has different value than that specified", logFile, config.getGeneric().getLogFile());
    }

    @Test
    public void testParseWithNullParam() throws Exception {
        CliConfig cliConfig = new CliConfig() {
            @Override
            public List<String> getParams() {
                String[] params = new String[] { 
                    "/generic/logFile=null",
                    "/replication/replSetName=rs1"
                };
                return Arrays.asList(params);
            }
        };
        Config config = CliConfigUtils.readConfig(cliConfig);
        
        Assert.assertEquals("Parameter has different value than that specified", null, config.getGeneric().getLogFile());
    }

    @Test
    public void testParseWithLogPackagesParam() throws Exception {
        final String logPackage = "com.torodb";
        final LogLevel logLevel = LogLevel.NONE;
        
        CliConfig cliConfig = new CliConfig() {
            @Override
            public List<String> getParams() {
                String[] params = new String[] { 
                    "/generic/logPackages/" + logPackage + "=" + logLevel.name(),
                    "/replication/replSetName=rs1"
                };
                return Arrays.asList(params);
            }
        };
        Config config = CliConfigUtils.readConfig(cliConfig);
        
        Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages() != null);
        Assert.assertTrue("/generic/logPackages/" + logPackage + " not defined", config.getGeneric().getLogPackages().get(logPackage) != null);
        Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric().getLogPackages().size());
        Assert.assertEquals("/generic/logPackages/" + logPackage + " has different value than that specified", logLevel, config.getGeneric().getLogPackages().get(logPackage));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParseWithPasswordParam() throws Exception {
        CliConfig cliConfig = new CliConfig() {
            @Override
            public List<String> getParams() {
                String[] params = new String[] { 
                    "/backend/postgres/password=toor"
                };
                return Arrays.asList(params);
            }
        };
        CliConfigUtils.readConfig(cliConfig);
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
		CliConfigUtils.readConfig(cliConfig);
	}

	@Test(expected=IllegalArgumentException.class)
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
		Config config = CliConfigUtils.readConfig(cliConfig);
		
		Assert.assertTrue("/generic not defined", config.getGeneric() != null);
		Assert.assertTrue("/replication not defined", config.getReplication() != null);
		Assert.assertTrue("/backend not defined", config.getBackend() != null);
		Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend().getBackendImplementation().getClass());
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
		CliConfigUtils.readConfig(cliConfig);
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
		Config config = CliConfigUtils.readConfig(cliConfig);
		
        Assert.assertTrue("/generic not defined", config.getGeneric() != null);
        Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages() != null);
        Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric().getLogPackages().get("com.torodb") != null);
        Assert.assertEquals("/generic/logLevel has different value than that specified", LogLevel.NONE, config.getGeneric().getLogLevel());
        Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric().getLogPackages().size());
        Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified", LogLevel.DEBUG, config.getGeneric().getLogPackages().get("com.torodb"));
        Assert.assertTrue("/replication not defined", config.getReplication() != null);
        Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1", config.getReplication().getReplSetName());
        Assert.assertEquals("/replication/role has different value than that specified", Role.HIDDEN_SLAVE, config.getReplication().getRole());
        Assert.assertEquals("/replication/syncSource has different value than that specified", "localhost:27017", config.getReplication().getSyncSource());
        Assert.assertTrue("/backend not defined", config.getBackend() != null);
        Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend().getBackendImplementation().getClass());
        Assert.assertTrue("/backend/postgres not identified as Postgres", config.getBackend().isPostgres());
        Assert.assertTrue("/backend/postgres not identified as Postgres Like", config.getBackend().isPostgresLike());
        Assert.assertEquals("/backend/postgres/host has different value than that specified", "localhost", config.getBackend().asPostgres().getHost());
        Assert.assertEquals("/backend/postgres/port has different value than that specified", Integer.valueOf(5432), config.getBackend().asPostgres().getPort());
        Assert.assertEquals("/backend/postgres/user has different value than that specified", "root", config.getBackend().asPostgres().getUser());
        Assert.assertEquals("/backend/postgres/password specified but should have not been read from parameters", null, config.getBackend().asPostgres().getPassword());
	}

	@Test
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
			        "/backend=null",
					"/backend/derby/host=localhost", 
					"/backend/derby/port=5432", 
					"/backend/derby/user=root", 
				};
				return Arrays.asList(params);
			}
		};
		Config config = CliConfigUtils.readConfig(cliConfig);
		
		Assert.assertTrue("/backend not defined", config.getBackend() != null);
		Assert.assertEquals("/backend/derby not defined", Derby.class, config.getBackend().getBackendImplementation().getClass());
		Assert.assertTrue("/backend/derby not identified as Derby", config.getBackend().isDerby());
		Assert.assertTrue("/backend/derby not identified as Derby Like", config.getBackend().isDerbyLike());
		Assert.assertEquals("/backend/derby/host has different value than that specified", "localhost", config.getBackend().asDerby().getHost());
		Assert.assertEquals("/backend/derby/port has different value than that specified", Integer.valueOf(5432), config.getBackend().asDerby().getPort());
		Assert.assertEquals("/backend/derby/user has different value than that specified", "root", config.getBackend().asDerby().getUser());
		Assert.assertEquals("/backend/derby/password specified but should have not been read from parameters", null, config.getBackend().asDerby().getPassword());
	}

    @Test
    public void testParseWithYAMLUsingDerby() throws Exception {
        CliConfig cliConfig = new CliConfig() {
            @Override
            public boolean hasConfFile() {
                return true;
            }
            @Override
            public InputStream getConfInputStream() {
                return ConfigTest.class.getResourceAsStream("/test-parse-with-yaml-using-derby.yml");
            }
        };
        Config config = CliConfigUtils.readConfig(cliConfig);
        
        Assert.assertTrue("/backend not defined", config.getBackend() != null);
        Assert.assertEquals("/backend/derby not defined", Derby.class, config.getBackend().getBackendImplementation().getClass());
        Assert.assertTrue("/backend/derby not identified as Derby", config.getBackend().isDerby());
        Assert.assertTrue("/backend/derby not identified as Derby Like", config.getBackend().isDerbyLike());
        Assert.assertEquals("/backend/derby/host has different value than that specified", "localhost", config.getBackend().asDerby().getHost());
        Assert.assertEquals("/backend/derby/port has different value than that specified", Integer.valueOf(5432), config.getBackend().asDerby().getPort());
        Assert.assertEquals("/backend/derby/user has different value than that specified", "root", config.getBackend().asDerby().getUser());
        Assert.assertEquals("/backend/derby/password specified but should have not been read from parameters", null, config.getBackend().asDerby().getPassword());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParseWithYAMLUsingPassword() throws Exception {
        CliConfig cliConfig = new CliConfig() {
            @Override
            public boolean hasConfFile() {
                return true;
            }
            @Override
            public InputStream getConfInputStream() {
                return ConfigTest.class.getResourceAsStream("/test-parse-with-yaml-using-password.yml");
            }
        };
        CliConfigUtils.readConfig(cliConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParseWithYAMLUsingEmptyProtocol() throws Exception {
        CliConfig cliConfig = new CliConfig() {
            @Override
            public boolean hasConfFile() {
                return true;
            }
            @Override
            public InputStream getConfInputStream() {
                return ConfigTest.class.getResourceAsStream("/test-parse-with-yaml-using-empty-replication.yml");
            }
        };
        CliConfigUtils.readConfig(cliConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParseWithYAMLUsingDoubleBackend() throws Exception {
        CliConfig cliConfig = new CliConfig() {
            @Override
            public boolean hasConfFile() {
                return true;
            }
            @Override
            public InputStream getConfInputStream() {
                return ConfigTest.class.getResourceAsStream("/test-parse-with-yaml-using-double-backend.yml");
            }
        };
        CliConfigUtils.readConfig(cliConfig);
    }

    @Test
    public void testParseWithXML() throws Exception {
        CliConfig cliConfig = new CliConfig() {
            @Override
            public boolean hasXmlConfFile() {
                return true;
            }
            @Override
            public InputStream getXmlConfInputStream() {
                return ConfigTest.class.getResourceAsStream("/test-parse-with-xml.xml");
            }
        };
        Config config = CliConfigUtils.readConfig(cliConfig);
        
        Assert.assertTrue("/generic not defined", config.getGeneric() != null);
        Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages() != null);
        Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric().getLogPackages().get("com.torodb") != null);
        Assert.assertEquals("/generic/logLevel has different value than that specified", LogLevel.NONE, config.getGeneric().getLogLevel());
        Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric().getLogPackages().size());
        Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified", LogLevel.DEBUG, config.getGeneric().getLogPackages().get("com.torodb"));
        Assert.assertTrue("/replication not defined", config.getReplication() != null);
        Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1", config.getReplication().getReplSetName());
        Assert.assertEquals("/replication/role has different value than that specified", Role.HIDDEN_SLAVE, config.getReplication().getRole());
        Assert.assertEquals("/replication/syncSource has different value than that specified", "localhost:27017", config.getReplication().getSyncSource());
        Assert.assertTrue("/backend not defined", config.getBackend() != null);
        Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend().getBackendImplementation().getClass());
        Assert.assertTrue("/backend/postgres not identified as Postgres", config.getBackend().isPostgres());
        Assert.assertTrue("/backend/postgres not identified as Postgres Like", config.getBackend().isPostgresLike());
        Assert.assertEquals("/backend/postgres/host has different value than that specified", "localhost", config.getBackend().asPostgres().getHost());
        Assert.assertEquals("/backend/postgres/port has different value than that specified", Integer.valueOf(5432), config.getBackend().asPostgres().getPort());
        Assert.assertEquals("/backend/postgres/user has different value than that specified", "root", config.getBackend().asPostgres().getUser());
        Assert.assertEquals("/backend/postgres/password has different value than that specified", null, config.getBackend().asPostgres().getPassword());
    }

    @Test
    public void testReplicationFiltering() throws Exception {
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
                        "/replication/include={torodb: [postgres, derby]}", 
                        "/replication/exclude={mongodb: {mmapv1, wiredtiger}}" 
                };
                return Arrays.asList(params);
            }
        };
        Config config = CliConfigUtils.readConfig(cliConfig);
        
        Assert.assertTrue("/generic not defined", config.getGeneric() != null);
        Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages() != null);
        Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric().getLogPackages().get("com.torodb") != null);
        Assert.assertEquals("/generic/logLevel has different value than that specified", LogLevel.NONE, config.getGeneric().getLogLevel());
        Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric().getLogPackages().size());
        Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified", LogLevel.DEBUG, config.getGeneric().getLogPackages().get("com.torodb"));
        Assert.assertTrue("/replication not defined", config.getReplication() != null);
        Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1", config.getReplication().getReplSetName());
        Assert.assertEquals("/replication/role has different value than that specified", Role.HIDDEN_SLAVE, config.getReplication().getRole());
        Assert.assertEquals("/replication/syncSource has different value than that specified", "localhost:27017", config.getReplication().getSyncSource());
        Assert.assertTrue("/replication/include not defined", config.getReplication().getInclude() != null);
        Assert.assertTrue("/replication/include/torodb not defined", config.getReplication().getInclude().get("torodb") != null);
        Assert.assertEquals("/replication/include/torodb has different value than that specified", 
                ImmutableMap.of("postgres", ImmutableList.of(), "derby", ImmutableList.of()), 
                config.getReplication().getInclude().get("torodb"));
        Assert.assertTrue("/replication/exclude not defined", config.getReplication().getExclude() != null);
        Assert.assertTrue("/replication/exclude/mongodb not defined", config.getReplication().getExclude().get("mongodb") != null);
        Assert.assertEquals("/replication/exclude/mongodb has different value than that specified", 
                ImmutableMap.of("mmapv1", ImmutableList.of(), "wiredtiger", ImmutableList.of()), 
                config.getReplication().getExclude().get("mongodb"));
        Assert.assertTrue("/backend not defined", config.getBackend() != null);
        Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend().getBackendImplementation().getClass());
        Assert.assertTrue("/backend/postgres not identified as Postgres", config.getBackend().isPostgres());
        Assert.assertTrue("/backend/postgres not identified as Postgres Like", config.getBackend().isPostgresLike());
        Assert.assertEquals("/backend/postgres/host has different value than that specified", "localhost", config.getBackend().asPostgres().getHost());
        Assert.assertEquals("/backend/postgres/port has different value than that specified", Integer.valueOf(5432), config.getBackend().asPostgres().getPort());
        Assert.assertEquals("/backend/postgres/user has different value than that specified", "root", config.getBackend().asPostgres().getUser());
        Assert.assertEquals("/backend/postgres/password specified but should have not been read from parameters", null, config.getBackend().asPostgres().getPassword());
    }

    @Test
    public void testReplicationFilteringWithIndexes() throws Exception {
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
                        "/replication/include={torodb: [{postgres: {name: awesome, unique: true}}, derby]}", 
                        "/replication/exclude={mongodb: [{mmapv1: {keys: {\"the.old.mmapv1\": 1}}}, wiredtiger]}" 
                };
                return Arrays.asList(params);
            }
        };
        Config config = CliConfigUtils.readConfig(cliConfig);
        
        Assert.assertTrue("/generic not defined", config.getGeneric() != null);
        Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages() != null);
        Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric().getLogPackages().get("com.torodb") != null);
        Assert.assertEquals("/generic/logLevel has different value than that specified", LogLevel.NONE, config.getGeneric().getLogLevel());
        Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric().getLogPackages().size());
        Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified", LogLevel.DEBUG, config.getGeneric().getLogPackages().get("com.torodb"));
        Assert.assertTrue("/replication not defined", config.getReplication() != null);
        Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1", config.getReplication().getReplSetName());
        Assert.assertEquals("/replication/role has different value than that specified", Role.HIDDEN_SLAVE, config.getReplication().getRole());
        Assert.assertEquals("/replication/syncSource has different value than that specified", "localhost:27017", config.getReplication().getSyncSource());
        Assert.assertTrue("/replication/include not defined", config.getReplication().getInclude() != null);
        Assert.assertTrue("/replication/include/torodb not defined", config.getReplication().getInclude().get("torodb") != null);
        Assert.assertEquals("/replication/include/torodb has different value than that specified", 
                ImmutableMap.of("postgres", ImmutableList.of(new IndexFilter("awesome", true, null)), "derby", ImmutableList.of()), 
                config.getReplication().getInclude().get("torodb"));
        Assert.assertTrue("/replication/exclude not defined", config.getReplication().getExclude() != null);
        Assert.assertTrue("/replication/exclude/mongodb not defined", config.getReplication().getExclude().get("mongodb") != null);
        Assert.assertEquals("/replication/exclude/mongodb has different value than that specified", 
                ImmutableMap.of("mmapv1", ImmutableList.of(new IndexFilter(null, null, ImmutableMap.<String, String>builder().put("the.old.mmapv1", "1").build())), "wiredtiger", ImmutableList.of()), 
                config.getReplication().getExclude().get("mongodb"));
        Assert.assertTrue("/backend not defined", config.getBackend() != null);
        Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend().getBackendImplementation().getClass());
        Assert.assertTrue("/backend/postgres not identified as Postgres", config.getBackend().isPostgres());
        Assert.assertTrue("/backend/postgres not identified as Postgres Like", config.getBackend().isPostgresLike());
        Assert.assertEquals("/backend/postgres/host has different value than that specified", "localhost", config.getBackend().asPostgres().getHost());
        Assert.assertEquals("/backend/postgres/port has different value than that specified", Integer.valueOf(5432), config.getBackend().asPostgres().getPort());
        Assert.assertEquals("/backend/postgres/user has different value than that specified", "root", config.getBackend().asPostgres().getUser());
        Assert.assertEquals("/backend/postgres/password specified but should have not been read from parameters", null, config.getBackend().asPostgres().getPassword());
    }

}
