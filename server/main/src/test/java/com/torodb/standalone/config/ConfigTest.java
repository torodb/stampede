/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.standalone.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.FilterList.IndexFilter;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.standalone.CliConfig;
import com.torodb.standalone.CliConfigUtils;
import com.torodb.standalone.config.model.Config;
import com.torodb.standalone.config.model.backend.derby.Derby;
import com.torodb.standalone.config.model.backend.postgres.Postgres;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

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
    ConfigUtils.readConfigFromYaml(Config.class, new String(byteArrayConsole
        .getByteArrayOutputStream().toByteArray()));
  }

  @Test
  public void testPrintXmlConf() throws Exception {
    ByteArrayConsole byteArrayConsole = new ByteArrayConsole();
    ConfigUtils.printXmlConfig(new Config(), byteArrayConsole);
    ConfigUtils.readConfigFromXml(Config.class, new String(byteArrayConsole
        .getByteArrayOutputStream().toByteArray()));
  }

  @Test
  public void testHelpParam() throws Exception {
    ResourceBundle configBundle = PropertyResourceBundle.getBundle("ConfigMessages");
    ConfigUtils.printParamDescriptionFromConfigSchema(Config.class, configBundle,
        new ByteArrayConsole(), 0);
  }

  @Test
  public void testParse() throws Exception {
    CliConfigUtils.readConfig(new CliConfig());
  }

  @Test
  public void testParseWithParam() throws Exception {
    File tempFile = File.createTempFile("torodb", ".log");
    tempFile.deleteOnExit();
    final String logFile = tempFile.getPath();

    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
          "/generic/logFile=" + logFile
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertEquals("Parameter has different value than that specified", logFile, config
        .getGeneric().getLogFile());
  }

  @Test
  public void testParseWithNullParam() throws Exception {
    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
          "/generic/logFile=null"
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertEquals("Parameter has different value than that specified", null, config
        .getGeneric().getLogFile());
  }

  @Test
  public void testParseWithLogPackagesParam() throws Exception {
    final String logPackage = "com.torodb";
    final LogLevel logLevel = LogLevel.NONE;

    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
          "/generic/logPackages/" + logPackage + "=" + logLevel.name()
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages()
        != null);
    Assert.assertTrue("/generic/logPackages/" + logPackage + " not defined", config.getGeneric()
        .getLogPackages().get(logPackage) != null);
    Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric()
        .getLogPackages().size());
    Assert.assertEquals("/generic/logPackages/" + logPackage
        + " has different value than that specified", logLevel, config.getGeneric().getLogPackages()
            .get(logPackage));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseWithPasswordParam() throws Exception {
    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
          "/backend/postgres/password=toor"
        };
        return Arrays.asList(params);
      }
    };
    CliConfigUtils.readConfig(cliConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseWithWrongTypeParam() throws Exception {
    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
          "/generic/logLevel=ALL"
        };
        return Arrays.asList(params);
      }
    };
    CliConfigUtils.readConfig(cliConfig);
  }

  @Test(expected = IllegalArgumentException.class)
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
    Assert.assertTrue("/protocol not defined", config.getProtocol() != null);
    Assert.assertTrue("/protocol/mongo not defined", config.getProtocol().getMongo() != null);
    Assert.assertTrue("/protocol/mongo/net not defined", config.getProtocol().getMongo().getNet()
        != null);
    Assert.assertTrue("/backend not defined", config.getBackend() != null);
    Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend()
        .getBackendImplementation().getClass());
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres", config.getBackend()
        .is(Postgres.class));
  }

  @Test(expected = IllegalArgumentException.class)
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
    Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages()
        != null);
    Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric()
        .getLogPackages().get("com.torodb") != null);
    Assert.assertEquals("/generic/logLevel has different value than that specified", LogLevel.NONE,
        config.getGeneric().getLogLevel());
    Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric()
        .getLogPackages().size());
    Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified",
        LogLevel.DEBUG, config.getGeneric().getLogPackages().get("com.torodb"));
    Assert.assertTrue("/protocol not defined", config.getProtocol() != null);
    Assert.assertTrue("/protocol/mongo not defined", config.getProtocol().getMongo() != null);
    Assert.assertTrue("/protocol/mongo/net not defined", config.getProtocol().getMongo().getNet()
        != null);
    Assert.assertTrue("/protocol/mongo/replication not defined", config.getProtocol().getMongo()
        .getReplication() != null);
    Assert.assertEquals("/protocol/mongo/net/port has different value than that specified", Integer
        .valueOf(27019), config.getProtocol().getMongo().getNet().getPort());
    Assert.assertEquals("/protocol/mongo/replication has not 1 element", 1, config.getProtocol()
        .getMongo().getReplication().size());
    Assert.assertEquals(
        "/protocol/mongo/replication/0/replSetName has different value than that specified", "rs1",
        config.getProtocol().getMongo().getReplication().get(0).getReplSetName());
    Assert
        .assertEquals("/protocol/mongo/replication/0/role has different value than that specified",
            Role.HIDDEN_SLAVE, config.getProtocol().getMongo().getReplication().get(0).getRole());
    Assert.assertEquals(
        "/protocol/mongo/replication/0/syncSource has different value than that specified",
        "localhost:27017", config.getProtocol().getMongo().getReplication().get(0).getSyncSource());
    Assert.assertTrue("/backend not defined", config.getBackend() != null);
    Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend()
        .getBackendImplementation().getClass());
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres", config.getBackend()
        .is(Postgres.class));
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres Like", config
        .getBackend().isLike(Postgres.class));
    Assert.assertEquals("/backend/postgres/host has different value than that specified",
        "localhost", config.getBackend().as(Postgres.class).getHost());
    Assert.assertEquals("/backend/postgres/port has different value than that specified", Integer
        .valueOf(5432), config.getBackend().as(Postgres.class).getPort());
    Assert.assertEquals("/backend/postgres/user has different value than that specified", "root",
        config.getBackend().as(Postgres.class).getUser());
    Assert.assertEquals(
        "/backend/postgres/password specified but should have not been read from parameters", null,
        config.getBackend().as(Postgres.class).getPassword());
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
        String[] params = new String[]{
          "/backend=null",
          "/backend/derby/host=localhost",
          "/backend/derby/port=5432",
          "/backend/derby/user=root",};
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/backend not defined", config.getBackend() != null);
    Assert.assertEquals("/backend/derby not defined", Derby.class, config.getBackend()
        .getBackendImplementation().getClass());
    Assert.assertTrue("/backend/derby not identified as AbstractDerby", config.getBackend().is(
        Derby.class));
    Assert.assertTrue("/backend/derby not identified as AbstractDerby Like", config.getBackend()
        .isLike(Derby.class));
    Assert.assertEquals("/backend/derby/host has different value than that specified", "localhost",
        config.getBackend().as(Derby.class).getHost());
    Assert.assertEquals("/backend/derby/port has different value than that specified", Integer
        .valueOf(5432), config.getBackend().as(Derby.class).getPort());
    Assert.assertEquals("/backend/derby/user has different value than that specified", "root",
        config.getBackend().as(Derby.class).getUser());
    Assert.assertEquals(
        "/backend/derby/password specified but should have not been read from parameters", null,
        config.getBackend().as(Derby.class).getPassword());
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
    Assert.assertEquals("/backend/derby not defined", Derby.class, config.getBackend()
        .getBackendImplementation().getClass());
    Assert.assertTrue("/backend/derby not identified as AbstractDerby", config.getBackend().is(
        Derby.class));
    Assert.assertTrue("/backend/derby not identified as AbstractDerby Like", config.getBackend()
        .isLike(Derby.class));
    Assert.assertEquals("/backend/derby/host has different value than that specified", "localhost",
        config.getBackend().as(Derby.class).getHost());
    Assert.assertEquals("/backend/derby/port has different value than that specified", Integer
        .valueOf(5432), config.getBackend().as(Derby.class).getPort());
    Assert.assertEquals("/backend/derby/user has different value than that specified", "root",
        config.getBackend().as(Derby.class).getUser());
    Assert.assertEquals(
        "/backend/derby/password specified but should have not been read from parameters", null,
        config.getBackend().as(Derby.class).getPassword());
  }

  @Test(expected = IllegalArgumentException.class)
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

  @Test(expected = IllegalArgumentException.class)
  public void testParseWithYAMLUsingEmptyProtocol() throws Exception {
    CliConfig cliConfig = new CliConfig() {
      @Override
      public boolean hasConfFile() {
        return true;
      }

      @Override
      public InputStream getConfInputStream() {
        return ConfigTest.class
            .getResourceAsStream("/test-parse-with-yaml-using-empty-protocol.yml");
      }
    };
    CliConfigUtils.readConfig(cliConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseWithYAMLUsingDoubleBackend() throws Exception {
    CliConfig cliConfig = new CliConfig() {
      @Override
      public boolean hasConfFile() {
        return true;
      }

      @Override
      public InputStream getConfInputStream() {
        return ConfigTest.class
            .getResourceAsStream("/test-parse-with-yaml-using-double-backend.yml");
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
    Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages()
        != null);
    Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric()
        .getLogPackages().get("com.torodb") != null);
    Assert.assertEquals("/generic/logLevel has different value than that specified", LogLevel.NONE,
        config.getGeneric().getLogLevel());
    Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric()
        .getLogPackages().size());
    Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified",
        LogLevel.DEBUG, config.getGeneric().getLogPackages().get("com.torodb"));
    Assert.assertTrue("/protocol not defined", config.getProtocol() != null);
    Assert.assertTrue("/protocol/mongo not defined", config.getProtocol().getMongo() != null);
    Assert.assertTrue("/protocol/mongo/net not defined", config.getProtocol().getMongo().getNet()
        != null);
    Assert.assertTrue("/protocol/mongo/replication not defined", config.getProtocol().getMongo()
        .getReplication() != null);
    Assert.assertEquals("/protocol/mongo/net/port has different value than that specified", Integer
        .valueOf(27019), config.getProtocol().getMongo().getNet().getPort());
    Assert.assertEquals("/protocol/mongo/replication has not 1 element", 1, config.getProtocol()
        .getMongo().getReplication().size());
    Assert.assertEquals(
        "/protocol/mongo/replication/0/replSetName has different value than that specified", "rs1",
        config.getProtocol().getMongo().getReplication().get(0).getReplSetName());
    Assert
        .assertEquals("/protocol/mongo/replication/0/role has different value than that specified",
            Role.HIDDEN_SLAVE, config.getProtocol().getMongo().getReplication().get(0).getRole());
    Assert.assertEquals(
        "/protocol/mongo/replication/0/syncSource has different value than that specified",
        "localhost:27017", config.getProtocol().getMongo().getReplication().get(0).getSyncSource());
    Assert.assertTrue("/backend not defined", config.getBackend() != null);
    Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend()
        .getBackendImplementation().getClass());
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres", config.getBackend()
        .is(Postgres.class));
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres Like", config
        .getBackend().isLike(Postgres.class));
    Assert.assertEquals("/backend/postgres/host has different value than that specified",
        "localhost", config.getBackend().as(Postgres.class).getHost());
    Assert.assertEquals("/backend/postgres/port has different value than that specified", Integer
        .valueOf(5432), config.getBackend().as(Postgres.class).getPort());
    Assert.assertEquals("/backend/postgres/user has different value than that specified", "root",
        config.getBackend().as(Postgres.class).getUser());
    Assert.assertEquals("/backend/postgres/password has different value than that specified", null,
        config.getBackend().as(Postgres.class).getPassword());
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
        String[] params = new String[]{
          "/protocol/mongo/replication/0/include={torodb: [postgres, derby]}",
          "/protocol/mongo/replication/0/exclude={mongodb: {mmapv1, wiredtiger}}"
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/generic not defined", config.getGeneric() != null);
    Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages()
        != null);
    Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric()
        .getLogPackages().get("com.torodb") != null);
    Assert.assertEquals("/generic/logLevel has different value than that specified", LogLevel.NONE,
        config.getGeneric().getLogLevel());
    Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric()
        .getLogPackages().size());
    Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified",
        LogLevel.DEBUG, config.getGeneric().getLogPackages().get("com.torodb"));
    Assert.assertTrue("/protocol not defined", config.getProtocol() != null);
    Assert.assertTrue("/protocol/mongo not defined", config.getProtocol().getMongo() != null);
    Assert.assertTrue("/protocol/mongo/net not defined", config.getProtocol().getMongo().getNet()
        != null);
    Assert.assertTrue("/protocol/mongo/replication not defined", config.getProtocol().getMongo()
        .getReplication() != null);
    Assert.assertEquals("/protocol/mongo/net/port has different value than that specified", Integer
        .valueOf(27019), config.getProtocol().getMongo().getNet().getPort());
    Assert.assertEquals("/protocol/mongo/replication has not 1 element", 1, config.getProtocol()
        .getMongo().getReplication().size());
    Assert.assertEquals(
        "/protocol/mongo/replication/0/replSetName has different value than that specified", "rs1",
        config.getProtocol().getMongo().getReplication().get(0).getReplSetName());
    Assert
        .assertEquals("/protocol/mongo/replication/0/role has different value than that specified",
            Role.HIDDEN_SLAVE, config.getProtocol().getMongo().getReplication().get(0).getRole());
    Assert.assertEquals(
        "/protocol/mongo/replication/0/syncSource has different value than that specified",
        "localhost:27017", config.getProtocol().getMongo().getReplication().get(0).getSyncSource());
    Assert.assertTrue("/protocol/mongo/replication/0/include not defined", config.getProtocol()
        .getMongo().getReplication().get(0).getInclude() != null);
    Assert.assertTrue("/protocol/mongo/replication/0/include/torodb not defined", config
        .getProtocol().getMongo().getReplication().get(0).getInclude().get("torodb") != null);
    Assert.assertEquals(
        "/protocol/mongo/replication/0/include/torodb has different value than that specified",
        ImmutableMap.of("postgres", ImmutableList.of(), "derby", ImmutableList.of()),
        config.getProtocol().getMongo().getReplication().get(0).getInclude().get("torodb"));
    Assert.assertTrue("/protocol/mongo/replication/0/exclude not defined", config.getProtocol()
        .getMongo().getReplication().get(0).getExclude() != null);
    Assert.assertTrue("/protocol/mongo/replication/0/exclude/mongodb not defined", config
        .getProtocol().getMongo().getReplication().get(0).getExclude().get("mongodb") != null);
    Assert.assertEquals(
        "/protocol/mongo/replication/0/exclude/mongodb has different value than that specified",
        ImmutableMap.of("mmapv1", ImmutableList.of(), "wiredtiger", ImmutableList.of()),
        config.getProtocol().getMongo().getReplication().get(0).getExclude().get("mongodb"));
    Assert.assertTrue("/backend not defined", config.getBackend() != null);
    Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend()
        .getBackendImplementation().getClass());
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres", config.getBackend()
        .is(Postgres.class));
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres Like", config
        .getBackend().isLike(Postgres.class));
    Assert.assertEquals("/backend/postgres/host has different value than that specified",
        "localhost", config.getBackend().as(Postgres.class).getHost());
    Assert.assertEquals("/backend/postgres/port has different value than that specified", Integer
        .valueOf(5432), config.getBackend().as(Postgres.class).getPort());
    Assert.assertEquals("/backend/postgres/user has different value than that specified", "root",
        config.getBackend().as(Postgres.class).getUser());
    Assert.assertEquals(
        "/backend/postgres/password specified but should have not been read from parameters", null,
        config.getBackend().as(Postgres.class).getPassword());
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
        String[] params = new String[]{
          "/protocol/mongo/replication/0/include={torodb: [{postgres: {name: awesome, unique: true}}, derby]}",
          "/protocol/mongo/replication/0/exclude={mongodb: [{mmapv1: {keys: {\"the.old.mmapv1\": 1}}}, wiredtiger]}"
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/generic not defined", config.getGeneric() != null);
    Assert.assertTrue("/generic/logPackages not defined", config.getGeneric().getLogPackages()
        != null);
    Assert.assertTrue("/generic/logPackages/com.torodb not defined", config.getGeneric()
        .getLogPackages().get("com.torodb") != null);
    Assert.assertEquals("/generic/logLevel has different value than that specified", LogLevel.NONE,
        config.getGeneric().getLogLevel());
    Assert.assertEquals("/generic/logPackages has not 1 entry", 1, config.getGeneric()
        .getLogPackages().size());
    Assert.assertEquals("/generic/logPackages/com.torodb has different value than that specified",
        LogLevel.DEBUG, config.getGeneric().getLogPackages().get("com.torodb"));
    Assert.assertTrue("/protocol not defined", config.getProtocol() != null);
    Assert.assertTrue("/protocol/mongo not defined", config.getProtocol().getMongo() != null);
    Assert.assertTrue("/protocol/mongo/net not defined", config.getProtocol().getMongo().getNet()
        != null);
    Assert.assertTrue("/protocol/mongo/replication not defined", config.getProtocol().getMongo()
        .getReplication() != null);
    Assert.assertEquals("/protocol/mongo/net/port has different value than that specified", Integer
        .valueOf(27019), config.getProtocol().getMongo().getNet().getPort());
    Assert.assertEquals("/protocol/mongo/replication has not 1 element", 1, config.getProtocol()
        .getMongo().getReplication().size());
    Assert.assertEquals(
        "/protocol/mongo/replication/0/replSetName has different value than that specified", "rs1",
        config.getProtocol().getMongo().getReplication().get(0).getReplSetName());
    Assert
        .assertEquals("/protocol/mongo/replication/0/role has different value than that specified",
            Role.HIDDEN_SLAVE, config.getProtocol().getMongo().getReplication().get(0).getRole());
    Assert.assertEquals(
        "/protocol/mongo/replication/0/syncSource has different value than that specified",
        "localhost:27017", config.getProtocol().getMongo().getReplication().get(0).getSyncSource());
    Assert.assertTrue("/protocol/mongo/replication/0/include not defined", config.getProtocol()
        .getMongo().getReplication().get(0).getInclude() != null);
    Assert.assertTrue("/protocol/mongo/replication/0/include/torodb not defined", config
        .getProtocol().getMongo().getReplication().get(0).getInclude().get("torodb") != null);
    Assert.assertEquals(
        "/protocol/mongo/replication/0/include/torodb has different value than that specified",
        ImmutableMap.of("postgres", ImmutableList.of(new IndexFilter("awesome", true, null)),
            "derby", ImmutableList.of()),
        config.getProtocol().getMongo().getReplication().get(0).getInclude().get("torodb"));
    Assert.assertTrue("/protocol/mongo/replication/0/exclude not defined", config.getProtocol()
        .getMongo().getReplication().get(0).getExclude() != null);
    Assert.assertTrue("/protocol/mongo/replication/0/exclude/mongodb not defined", config
        .getProtocol().getMongo().getReplication().get(0).getExclude().get("mongodb") != null);
    Assert.assertEquals(
        "/protocol/mongo/replication/0/exclude/mongodb has different value than that specified",
        ImmutableMap.of("mmapv1", ImmutableList.of(new IndexFilter(null, null, ImmutableMap
            .<String, String>builder().put("the.old.mmapv1", "1").build())), "wiredtiger",
            ImmutableList.of()),
        config.getProtocol().getMongo().getReplication().get(0).getExclude().get("mongodb"));
    Assert.assertTrue("/backend not defined", config.getBackend() != null);
    Assert.assertEquals("/backend/postgres not defined", Postgres.class, config.getBackend()
        .getBackendImplementation().getClass());
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres", config.getBackend()
        .is(Postgres.class));
    Assert.assertTrue("/backend/postgres not identified as AbstractPostgres Like", config
        .getBackend().isLike(Postgres.class));
    Assert.assertEquals("/backend/postgres/host has different value than that specified",
        "localhost", config.getBackend().as(Postgres.class).getHost());
    Assert.assertEquals("/backend/postgres/port has different value than that specified", Integer
        .valueOf(5432), config.getBackend().as(Postgres.class).getPort());
    Assert.assertEquals("/backend/postgres/user has different value than that specified", "root",
        config.getBackend().as(Postgres.class).getUser());
    Assert.assertEquals(
        "/backend/postgres/password specified but should have not been read from parameters", null,
        config.getBackend().as(Postgres.class).getPassword());
  }

}
