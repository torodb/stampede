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

package com.torodb.stampede.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.FilterList.IndexFilter;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.stampede.CliConfig;
import com.torodb.stampede.CliConfigUtils;
import com.torodb.stampede.config.model.Config;
import com.torodb.stampede.config.model.backend.postgres.Postgres;
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
    Config config = new Config();
    ConfigUtils.printYamlConfig(config, byteArrayConsole);
    ConfigUtils.readConfigFromYaml(Config.class, new String(byteArrayConsole
        .getByteArrayOutputStream().toByteArray()));
  }

  @Test
  public void testPrintXmlConf() throws Exception {
    ByteArrayConsole byteArrayConsole = new ByteArrayConsole();
    Config config = new Config();
    ConfigUtils.printXmlConfig(config, byteArrayConsole);
    ConfigUtils.readConfigFromXml(Config.class, new String(byteArrayConsole
        .getByteArrayOutputStream().toByteArray()));
  }

  @Test
  public void testHelpParam() throws Exception {
    ResourceBundle cliBundle = PropertyResourceBundle.getBundle("CliMessages");
    ConfigUtils.printParamDescriptionFromConfigSchema(Config.class, cliBundle,
        new ByteArrayConsole(), 0);
  }

  @Test
  public void testParse() throws Exception {
    CliConfig cliConfig = new CliConfig();
    CliConfigUtils.readConfig(cliConfig);
  }

  @Test
  public void testParseWithParam() throws Exception {
    File tempFile = File.createTempFile("torodb", ".log");
    tempFile.deleteOnExit();
    final String file = tempFile.getPath();

    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
          "/logging/file=" + file
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertEquals("Parameter has different value than that specified", file, config
        .getLogging().getFile());
  }

  @Test
  public void testParseWithNullParam() throws Exception {
    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
          "/logging/file=null"
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertEquals("Parameter has different value than that specified", null, config
        .getLogging().getFile());
  }

  @Test
  public void testParseWithLogPackagesParam() throws Exception {
    final String logPackage = "com.torodb";
    final LogLevel level = LogLevel.NONE;

    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
          "/logging/packages/" + logPackage + "=" + level.name()
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/logging/packages not defined", config.getLogging().getPackages() != null);
    Assert.assertTrue("/logging/packages/" + logPackage + " not defined", config.getLogging()
        .getPackages().get(logPackage) != null);
    Assert.assertEquals("/logging/packages has not 1 entry", 1, config.getLogging().getPackages()
        .size());
    Assert.assertEquals("/logging/packages/" + logPackage
        + " has different value than that specified", level, config.getLogging().getPackages().get(
            logPackage));
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
          "/logging/level=ALL"
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

    Assert.assertTrue("/logging not defined", config.getLogging() != null);
    Assert.assertTrue("/replication not defined", config.getReplication() != null);
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

    Assert.assertTrue("/logging not defined", config.getLogging() != null);
    Assert.assertTrue("/logging/packages not defined", config.getLogging().getPackages() != null);
    Assert.assertTrue("/logging/packages/com.torodb not defined", config.getLogging().getPackages()
        .get("com.torodb") != null);
    Assert.assertEquals("/logging/level has different value than that specified", LogLevel.NONE,
        config.getLogging().getLevel());
    Assert.assertEquals("/logging/packages has not 1 entry", 1, config.getLogging().getPackages()
        .size());
    Assert.assertEquals("/logging/packages/com.torodb has different value than that specified",
        LogLevel.DEBUG, config.getLogging().getPackages().get("com.torodb"));
    Assert.assertTrue("/replication not defined", config.getReplication() != null);
    Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1",
        config.getReplication().getReplSetName());
    Assert.assertEquals("/replication/role has different value than that specified",
        Role.HIDDEN_SLAVE, config.getReplication().getRole());
    Assert.assertEquals("/replication/syncSource has different value than that specified",
        "localhost:27017", config.getReplication().getSyncSource());
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
        return ConfigTest.class.getResourceAsStream(
            "/test-parse-with-yaml-using-empty-replication.yml");
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

    Assert.assertTrue("/logging not defined", config.getLogging() != null);
    Assert.assertTrue("/logging/packages not defined", config.getLogging().getPackages() != null);
    Assert.assertTrue("/logging/packages/com.torodb not defined", config.getLogging().getPackages()
        .get("com.torodb") != null);
    Assert.assertEquals("/logging/level has different value than that specified", LogLevel.NONE,
        config.getLogging().getLevel());
    Assert.assertEquals("/logging/packages has not 1 entry", 1, config.getLogging().getPackages()
        .size());
    Assert.assertEquals("/logging/packages/com.torodb has different value than that specified",
        LogLevel.DEBUG, config.getLogging().getPackages().get("com.torodb"));
    Assert.assertTrue("/replication not defined", config.getReplication() != null);
    Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1",
        config.getReplication().getReplSetName());
    Assert.assertEquals("/replication/role has different value than that specified",
        Role.HIDDEN_SLAVE, config.getReplication().getRole());
    Assert.assertEquals("/replication/syncSource has different value than that specified",
        "localhost:27017", config.getReplication().getSyncSource());
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
          "/replication/include={torodb: [postgres, derby]}",
          "/replication/exclude={mongodb: {mmapv1, wiredtiger}}"
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/logging not defined", config.getLogging() != null);
    Assert.assertTrue("/logging/packages not defined", config.getLogging().getPackages() != null);
    Assert.assertTrue("/logging/packages/com.torodb not defined", config.getLogging().getPackages()
        .get("com.torodb") != null);
    Assert.assertEquals("/logging/level has different value than that specified", LogLevel.NONE,
        config.getLogging().getLevel());
    Assert.assertEquals("/logging/packages has not 1 entry", 1, config.getLogging().getPackages()
        .size());
    Assert.assertEquals("/logging/packages/com.torodb has different value than that specified",
        LogLevel.DEBUG, config.getLogging().getPackages().get("com.torodb"));
    Assert.assertTrue("/replication not defined", config.getReplication() != null);
    Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1",
        config.getReplication().getReplSetName());
    Assert.assertEquals("/replication/role has different value than that specified",
        Role.HIDDEN_SLAVE, config.getReplication().getRole());
    Assert.assertEquals("/replication/syncSource has different value than that specified",
        "localhost:27017", config.getReplication().getSyncSource());
    Assert.assertTrue("/replication/include not defined", config.getReplication().getInclude()
        != null);
    Assert.assertTrue("/replication/include/torodb not defined", config.getReplication()
        .getInclude().get("torodb") != null);
    Assert.assertEquals("/replication/include/torodb has different value than that specified",
        ImmutableMap.of("postgres", ImmutableList.of(), "derby", ImmutableList.of()),
        config.getReplication().getInclude().get("torodb"));
    Assert.assertTrue("/replication/exclude not defined", config.getReplication().getExclude()
        != null);
    Assert.assertTrue("/replication/exclude/mongodb not defined", config.getReplication()
        .getExclude().get("mongodb") != null);
    Assert.assertEquals("/replication/exclude/mongodb has different value than that specified",
        ImmutableMap.of("mmapv1", ImmutableList.of(), "wiredtiger", ImmutableList.of()),
        config.getReplication().getExclude().get("mongodb"));
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
          "/replication/include={torodb: [{postgres: {name: awesome, unique: true}}, derby]}",
          "/replication/exclude={mongodb: [{mmapv1: {keys: {\"the.old.mmapv1\": 1}}}, wiredtiger]}"
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/logging not defined", config.getLogging() != null);
    Assert.assertTrue("/logging/packages not defined", config.getLogging().getPackages() != null);
    Assert.assertTrue("/logging/packages/com.torodb not defined", config.getLogging().getPackages()
        .get("com.torodb") != null);
    Assert.assertEquals("/logging/level has different value than that specified", LogLevel.NONE,
        config.getLogging().getLevel());
    Assert.assertEquals("/logging/packages has not 1 entry", 1, config.getLogging().getPackages()
        .size());
    Assert.assertEquals("/logging/packages/com.torodb has different value than that specified",
        LogLevel.DEBUG, config.getLogging().getPackages().get("com.torodb"));
    Assert.assertTrue("/replication not defined", config.getReplication() != null);
    Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1",
        config.getReplication().getReplSetName());
    Assert.assertEquals("/replication/role has different value than that specified",
        Role.HIDDEN_SLAVE, config.getReplication().getRole());
    Assert.assertEquals("/replication/syncSource has different value than that specified",
        "localhost:27017", config.getReplication().getSyncSource());
    Assert.assertTrue("/replication/include not defined", config.getReplication().getInclude()
        != null);
    Assert.assertTrue("/replication/include/torodb not defined", config.getReplication()
        .getInclude().get("torodb") != null);
    Assert.assertEquals("/replication/include/torodb has different value than that specified",
        ImmutableMap.of("postgres", ImmutableList.of(new IndexFilter("awesome", true, null)),
            "derby", ImmutableList.of()),
        config.getReplication().getInclude().get("torodb"));
    Assert.assertTrue("/replication/exclude not defined", config.getReplication().getExclude()
        != null);
    Assert.assertTrue("/replication/exclude/mongodb not defined", config.getReplication()
        .getExclude().get("mongodb") != null);
    Assert.assertEquals("/replication/exclude/mongodb has different value than that specified",
        ImmutableMap.of("mmapv1", ImmutableList.of(new IndexFilter(null, null, ImmutableMap
            .<String, String>builder().put("the.old.mmapv1", "1").build())), "wiredtiger",
            ImmutableList.of()),
        config.getReplication().getExclude().get("mongodb"));
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
