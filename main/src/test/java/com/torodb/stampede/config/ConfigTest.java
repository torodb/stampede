/*
 * ToroDB Stampede
 * Copyright © 2016 8Kdata Technology (www.8kdata.com)
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
import com.torodb.packaging.config.model.common.ListOfStringWithDefault;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.AuthMode;
import com.torodb.packaging.config.model.protocol.mongo.FilterList.IndexFilter;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.stampede.CliConfig;
import com.torodb.stampede.CliConfigUtils;
import com.torodb.stampede.config.model.Config;
import com.torodb.stampede.config.model.backend.postgres.Postgres;
import com.torodb.stampede.config.model.mongo.replication.ShardReplication;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

public class ConfigTest {

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
  public void testPrintConfMultipleSyncSources() throws Exception {
    ByteArrayConsole byteArrayConsole = new ByteArrayConsole();
    Config config = new Config();
    config.getReplication().setSyncSource(new ListOfStringWithDefault(
        Arrays.asList("localhost:27017", "localhost:27018"), false));
    ConfigUtils.printYamlConfig(config, byteArrayConsole);
    ConfigUtils.readConfigFromYaml(Config.class, new String(byteArrayConsole
        .getByteArrayOutputStream().toByteArray()));
  }

  @Test
  public void testPrintXmlConfMultipleSyncSources() throws Exception {
    ByteArrayConsole byteArrayConsole = new ByteArrayConsole();
    Config config = new Config();
    config.getReplication().setSyncSource(new ListOfStringWithDefault(
        Arrays.asList("localhost:27017", "localhost:27018"), false));
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
    CliConfigUtils.readConfig(cliConfig);
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
        config.getReplication().getReplSetName().value());
    Assert.assertEquals("/replication/role has different value than that specified",
        Role.HIDDEN_SLAVE, config.getReplication().getRole().value());
    Assert.assertEquals("/replication/syncSource has different value than that specified",
        Arrays.asList("localhost:27017"), config.getReplication().getSyncSource().value());
    Assert.assertNotNull("/replication/include not defined", config.getReplication().getInclude());
    Assert.assertTrue("/replication/include empty", !config.getReplication().getInclude().isEmpty());
    Assert.assertNotNull("/replication/exclude not defined", config.getReplication().getExclude());
    Assert.assertTrue("/replication/exclude empty", !config.getReplication().getExclude().isEmpty());
    Assert.assertNotNull("/replication/shards not defined", config.getReplication().getShardList());
    Assert.assertTrue("/replication/shards not empty", config.getReplication().getShardList().isEmpty());
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
        config.getReplication().getReplSetName().value());
    Assert.assertEquals("/replication/role has different value than that specified",
        Role.HIDDEN_SLAVE, config.getReplication().getRole().value());
    Assert.assertEquals("/replication/syncSource has different value than that specified",
        Arrays.asList("localhost:27017"), config.getReplication().getSyncSource().value());
    Assert.assertTrue("/replication/include defined", config.getReplication().getInclude()
        == null);
    Assert.assertTrue("/replication/exclude defined", config.getReplication().getExclude()
        == null);
    Assert.assertTrue("/replication/shards not defined", config.getReplication().getShardList()
        != null);
    Assert.assertTrue("/replication/shards not empty", config.getReplication().getShardList().isEmpty());
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
      public List<String> getParams() {
        String[] params = new String[]{
          "/replication/include={torodb: [postgres, derby]}",
          "/replication/exclude={mongodb: {mmapv1, wiredtiger}}"
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/replication not defined", config.getReplication() != null);
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
  }

  @Test
  public void testReplicationFilteringWithIndexes() throws Exception {
    CliConfig cliConfig = new CliConfig() {
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

    Assert.assertTrue("/replication not defined", config.getReplication() != null);
    Assert.assertEquals("/replication/replSetName has different value than that specified", "rs1",
        config.getReplication().getReplSetName().value());
    Assert.assertEquals("/replication/role has different value than that specified",
        Role.HIDDEN_SLAVE, config.getReplication().getRole().value());
    Assert.assertEquals("/replication/syncSource has different value than that specified",
        Arrays.asList("localhost:27017"), config.getReplication().getSyncSource().value());
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
  }

  @Test
  public void testReplicationFilteringWithShards() throws Exception {
    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
            "/replication/shards=[" 
                + "{syncSource: 'localhost:27020', replSetName: shard1},"
                + "{syncSource: 'localhost:27030', replSetName: shard2},"
                + "{syncSource: 'localhost:27040', replSetName: shard3}" 
                + "]",
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/replication/shards not defined", config.getReplication().getShardList()
        != null);
    Assert.assertTrue("/replication/shards empty", !config.getReplication().getShardList().isEmpty());
    Assert.assertEquals("/replication/shards has different size that specified", 3, config.getReplication().getShardList().size());
    Assert.assertEquals("/replication/shards/0/syncSource has different value than that specified", 
        Arrays.asList("localhost:27020"), config.getReplication().getShardList().get(0).getSyncSource().value());
    Assert.assertEquals("/replication/shards/0/replSetName has different value than that specified", 
        "shard1", config.getReplication().getShardList().get(0).getReplSetName().value());
    Assert.assertEquals("/replication/shards/1/syncSource has different value than that specified", 
        Arrays.asList("localhost:27030"), config.getReplication().getShardList().get(1).getSyncSource().value());
    Assert.assertEquals("/replication/shards/1/replSetName has different value than that specified", 
        "shard2", config.getReplication().getShardList().get(1).getReplSetName().value());
    Assert.assertEquals("/replication/shards/2/syncSource has different value than that specified", 
        Arrays.asList("localhost:27040"), config.getReplication().getShardList().get(2).getSyncSource().value());
    Assert.assertEquals("/replication/shards/2/replSetName has different value than that specified", 
        "shard3", config.getReplication().getShardList().get(2).getReplSetName().value());
  }

  @Test
  public void testReplicationFilteringWithShardsMerged() throws Exception {
    CliConfig cliConfig = new CliConfig() {
      @Override
      public List<String> getParams() {
        String[] params = new String[]{
            "/replication/auth/mode=negotiate",
            "/replication/auth/user=userShard",
            "/replication/ssl/enabled=true",
            "/replication/shards=[" 
                + "{syncSource: 'localhost:27020', replSetName: shard1, auth: {mode: disabled}, ssl: {enabled: false}},"
                + "{syncSource: 'localhost:27030', replSetName: shard2, auth: {source: usersShard1}},"
                + "{syncSource: 'localhost:27040', replSetName: shard3, auth: {source: usersShard2, user: userShard2}}"
                + "]",
        };
        return Arrays.asList(params);
      }
    };
    Config config = CliConfigUtils.readConfig(cliConfig);

    Assert.assertTrue("/replication/shards not defined", config.getReplication().getShardList()
        != null);
    List<ShardReplication> shards = config.getReplication().getShardList()
        .stream()
        .map(shard -> config.getReplication().mergeWith(shard))
        .collect(Collectors.toList());
    
    Assert.assertEquals("/replication/shards has different size that specified", 3, shards.size());
    Assert.assertEquals("/replication/shards/0/syncSource has different value than that specified", 
        Arrays.asList("localhost:27020"), shards.get(0).getSyncSource().value());
    Assert.assertEquals("/replication/shards/0/replSetName has different value than that specified", 
        "shard1", shards.get(0).getReplSetName().value());
    Assert.assertEquals("/replication/shards/0/auth/mode has different value than that specified", 
        AuthMode.disabled, shards.get(0).getAuth().getMode().value());
    Assert.assertEquals("/replication/shards/0/ssl/enabled has different value than that specified", 
        false, shards.get(0).getSsl().getEnabled().value());
    Assert.assertEquals("/replication/shards/1/syncSource has different value than that specified", 
        Arrays.asList("localhost:27030"), shards.get(1).getSyncSource().value());
    Assert.assertEquals("/replication/shards/1/replSetName has different value than that specified", 
        "shard2", shards.get(1).getReplSetName().value());
    Assert.assertEquals("/replication/shards/1/auth/mode has different value than that specified", 
        AuthMode.negotiate, shards.get(1).getAuth().getMode().value());
    Assert.assertEquals("/replication/shards/1/auth/source has different value than that specified", 
        "usersShard1", shards.get(1).getAuth().getSource().value());
    Assert.assertEquals("/replication/shards/1/auth/user has different value than that specified", 
        "userShard", shards.get(1).getAuth().getUser().value());
    Assert.assertEquals("/replication/shards/1/ssl/enabled has different value than that specified", 
        true, shards.get(1).getSsl().getEnabled().value());
    Assert.assertEquals("/replication/shards/2/syncSource has different value than that specified", 
        Arrays.asList("localhost:27040"), shards.get(2).getSyncSource().value());
    Assert.assertEquals("/replication/shards/2/replSetName has different value than that specified", 
        "shard3", shards.get(2).getReplSetName().value());
    Assert.assertEquals("/replication/shards/2/auth/mode has different value than that specified", 
        AuthMode.negotiate, shards.get(2).getAuth().getMode().value());
    Assert.assertEquals("/replication/shards/2/auth/source has different value than that specified", 
        "usersShard2", shards.get(2).getAuth().getSource().value());
    Assert.assertEquals("/replication/shards/2/auth/user has different value than that specified", 
        "userShard2", shards.get(2).getAuth().getUser().value());
    Assert.assertEquals("/replication/shards/2/ssl/enabled has different value than that specified", 
        true, shards.get(1).getSsl().getEnabled().value());
  }

}
