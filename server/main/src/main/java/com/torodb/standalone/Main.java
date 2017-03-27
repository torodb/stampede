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

package com.torodb.standalone;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.internal.Console;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.guice.EssentialModule;
import com.torodb.core.logging.ComponentLoggerFactory;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.metrics.MetricsConfig;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.wp.MongoDbWpBundle;
import com.torodb.mongodb.wp.MongoDbWpConfig;
import com.torodb.packaging.config.model.backend.BackendPasswordConfig;
import com.torodb.packaging.config.model.backend.derby.AbstractDerby;
import com.torodb.packaging.config.model.backend.postgres.AbstractPostgres;
import com.torodb.packaging.config.model.protocol.mongo.AbstractShardReplication;
import com.torodb.packaging.config.model.protocol.mongo.MongoPasswordConfig;
import com.torodb.packaging.config.model.protocol.mongo.Net;
import com.torodb.packaging.config.util.BackendImplementationVisitor;
import com.torodb.packaging.config.util.BundleFactory;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.util.Log4jUtils;
import com.torodb.standalone.config.model.Config;
import com.torodb.standalone.config.model.backend.Backend;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

/**
 * ToroDB Server entry point.
 */
public class Main {

  private static final LoggerFactory LOGGER_FACTORY = new ComponentLoggerFactory("SERVER");
  private static final Logger LOGGER = LOGGER_FACTORY.apply(Main.class);

  /**
   * The main method that runs ToroDB Server.
   */
  public static void main(String[] args) throws Exception {
    Console console = JCommander.getConsole();

    ResourceBundle cliBundle = PropertyResourceBundle.getBundle("CliMessages");
    final CliConfig cliConfig = new CliConfig();
    @SuppressWarnings("checkstyle:LocalVariableName")
    JCommander jCommander = new JCommander(cliConfig, cliBundle, args);
    jCommander.setColumnSize(Integer.MAX_VALUE);

    if (cliConfig.isHelp()) {
      jCommander.usage();
      System.exit(0);
    }

    if (cliConfig.isHelpParam()) {
      console.println(cliBundle.getString("help-param-header"));
      ResourceBundle configBundle = PropertyResourceBundle.getBundle("ConfigMessages");
      ConfigUtils.printParamDescriptionFromConfigSchema(Config.class, configBundle, console, 0);
      System.exit(0);
    }

    final Config config = CliConfigUtils.readConfig(cliConfig);

    if (cliConfig.isPrintConfig()) {
      ConfigUtils.printYamlConfig(config, console);

      System.exit(0);
    }

    if (cliConfig.isPrintXmlConfig()) {
      ConfigUtils.printXmlConfig(config, console);

      System.exit(0);
    }

    configureLogger(cliConfig, config);

    parseToropassFile(config);
    
    if (config.getProtocol().getMongo().getReplication() != null) {
      List<AbstractShardReplication> shards;
      if (config.getProtocol().getMongo().getReplication().getShards().isEmpty()) {
        shards = Lists.newArrayList(config.getProtocol().getMongo().getReplication());
      } else {
        shards = config.getProtocol().getMongo().getReplication().getShards()
            .stream()
            .map(shard -> (AbstractShardReplication) 
                config.getProtocol().getMongo().getReplication().mergeWith(shard))
            .collect(Collectors.toList());
      }
      for (AbstractShardReplication shard : shards) {
        if (shard.getAuth().getUser() != null) {
          HostAndPort syncSource = HostAndPort.fromString(shard.getSyncSource().value())
              .withDefaultPort(27017);
          ConfigUtils.parseMongopassFile(new MongoPasswordConfig() {

            @Override
            public void setPassword(String password) {
              shard.getAuth().setPassword(password);
            }

            @Override
            public String getUser() {
              return shard.getAuth().getUser().value();
            }

            @Override
            public Integer getPort() {
              return syncSource.getPort();
            }

            @Override
            public String getPassword() {
              return shard.getAuth().getPassword();
            }

            @Override
            public String getMongopassFile() {
              return config.getProtocol().getMongo().getMongopassFile();
            }

            @Override
            public String getHost() {
              return syncSource.getHost();
            }

            @Override
            public String getDatabase() {
              return shard.getAuth().getSource().value();
            }
          }, LOGGER);
        }
      }
    }

    if (config.getBackend().isLike(AbstractPostgres.class)) {
      AbstractPostgres postgres = config.getBackend().as(AbstractPostgres.class);

      if (cliConfig.isAskForPassword()) {
        console.print("Database user " + postgres.getUser() + " password:");
        postgres.setPassword(readPwd());
      }
    } else if (config.getBackend().isLike(AbstractDerby.class)) {
      AbstractDerby derby = config.getBackend().as(AbstractDerby.class);

      if (cliConfig.isAskForPassword()) {
        console.print("Database user " + derby.getUser() + " password:");
        derby.setPassword(readPwd());
      }
    }

    try {
      Clock clock = Clock.systemDefaultZone();
      Service server;
      if (config.getProtocol().getMongo().getReplication() == null || config.getProtocol()
          .getMongo().getReplication().getShards().isEmpty()) {
        Service toroDbServer = new ServerService(createServerConfig(config));

        toroDbServer.startAsync();
        toroDbServer.awaitRunning();

        server = toroDbServer;
      } else {
        throw new UnsupportedOperationException("Replication not supported yet!");
      }

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        server.stopAsync();
        server.awaitTerminated();
      }));
    } catch (CreationException ex) {
      ex.getErrorMessages().stream().forEach(m -> {
        if (m.getCause() != null) {
          LOGGER.error(m.getCause().getMessage());
        } else {
          LOGGER.error(m.getMessage());
        }
      });
      System.exit(1);
    } catch (Throwable ex) {
      LOGGER.error("Fatal error on initialization", ex);
      Throwable rootCause = Throwables.getRootCause(ex);
      String causeMessage = rootCause.getMessage();
      JCommander.getConsole().println("Fatal error while ToroDB was starting: " + causeMessage);
      System.exit(1);
    }
  }

  private static ServerConfig createServerConfig(Config config) {
    Clock clock = Clock.systemDefaultZone();

    MetricsConfig metricsConfig = () -> true;

    Backend backendConfig = config.getBackend();
    backendConfig.setConnectionPoolConfig(config.getGeneric());

    return new ServerConfig(
        Guice.createInjector(new EssentialModule(
            new ComponentLoggerFactory("LIFECYCLE"),
            metricsConfig,
            clock)
        ),
        generalConfig -> BundleFactory.createBackendBundle(
            backendConfig,
            generalConfig
        ),
        getSelfHostAndPort(config),
        (generalConfig, coreBundle) -> createMongoDbWpBundle(config, coreBundle, generalConfig),
        LOGGER_FACTORY
    );
  }

  private static HostAndPort getSelfHostAndPort(Config config) {
    Net net = config.getProtocol().getMongo().getNet();
    return HostAndPort.fromParts(net.getBindIp(), net.getPort());
  }

  private static MongoDbWpBundle createMongoDbWpBundle(
      Config config, MongoDbCoreBundle coreBundle, BundleConfig generalConfig) {
    int port = config.getProtocol().getMongo().getNet().getPort();
    return new MongoDbWpBundle(new MongoDbWpConfig(coreBundle, port, generalConfig));
  }

  private static void configureLogger(CliConfig cliConfig, Config config) {
    if (cliConfig.hasConfFile()) {
      if (config.getGeneric().getLogLevel() != null) {
        Log4jUtils.setRootLevel(config.getGeneric().getLogLevel());
      }

      if (config.getGeneric().getLogPackages() != null) {
        Log4jUtils.setLogPackages(config.getGeneric().getLogPackages());
      }

      if (config.getGeneric().getLogFile() != null) {
        Log4jUtils.appendToLogFile(config.getGeneric().getLogFile());
      }
    }
    // If not specified in configuration YAML then the log4j2.xml is used instead (by default)
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

  private static void parseToropassFile(Config config) {
    BackendImplementationVisitor<?, ?> visitor = new BackendImplementationVisitor<Void, Void>() {
      @Override
      public Void visit(AbstractDerby value, Void arg) {
        parseToropassFile(value);
        return null;
      }

      @Override
      public Void visit(AbstractPostgres value, Void arg) {
        parseToropassFile(value);
        return null;
      }

      public void parseToropassFile(BackendPasswordConfig value) {
        try {
          ConfigUtils.parseToropassFile(value, LOGGER);
        } catch (Exception ex) {
          throw new SystemException(ex);
        }
      }
    };

    config.getBackend().getBackendImplementation().accept(visitor, null);
  }
}
