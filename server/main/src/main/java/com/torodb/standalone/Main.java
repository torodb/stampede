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
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.CreationException;
import com.torodb.core.exceptions.SystemException;
import com.torodb.packaging.config.model.backend.BackendPasswordConfig;
import com.torodb.packaging.config.model.backend.derby.AbstractDerby;
import com.torodb.packaging.config.model.backend.postgres.AbstractPostgres;
import com.torodb.packaging.config.model.protocol.mongo.AbstractReplication;
import com.torodb.packaging.config.model.protocol.mongo.MongoPasswordConfig;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.config.visitor.BackendImplementationVisitor;
import com.torodb.packaging.util.Log4jUtils;
import com.torodb.standalone.config.model.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

/**
 * ToroDB's entry point
 */
public class Main {

  private static final Logger LOGGER = LogManager.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    Console console = JCommander.getConsole();

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

    config.getBackend().getBackendImplementation().accept(new BackendImplementationVisitor() {
      @Override
      public void visit(AbstractDerby value) {
        parseToropassFile(value);
      }

      @Override
      public void visit(AbstractPostgres value) {
        parseToropassFile(value);
      }

      public void parseToropassFile(BackendPasswordConfig value) {
        try {
          ConfigUtils.parseToropassFile(value);
        } catch (Exception ex) {
          throw new SystemException(ex);
        }
      }
    });
    if (config.getProtocol().getMongo().getReplication() != null) {
      for (AbstractReplication replication : config.getProtocol().getMongo().getReplication()) {
        if (replication.getAuth().getUser() != null) {
          HostAndPort syncSource = HostAndPort.fromString(replication.getSyncSource())
              .withDefaultPort(27017);
          ConfigUtils.parseMongopassFile(new MongoPasswordConfig() {

            @Override
            public void setPassword(String password) {
              replication.getAuth().setPassword(password);
            }

            @Override
            public String getUser() {
              return replication.getAuth().getUser();
            }

            @Override
            public Integer getPort() {
              return syncSource.getPort();
            }

            @Override
            public String getPassword() {
              return replication.getAuth().getPassword();
            }

            @Override
            public String getMongopassFile() {
              return config.getProtocol().getMongo().getMongopassFile();
            }

            @Override
            public String getHost() {
              return syncSource.getHostText();
            }

            @Override
            public String getDatabase() {
              return replication.getAuth().getSource();
            }
          });
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
          .getMongo().getReplication().isEmpty()) {
        Service toroDbServer = ToroDbBootstrap.createStandaloneService(config, clock);

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
}
