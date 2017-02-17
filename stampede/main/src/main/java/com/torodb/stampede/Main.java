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

package com.torodb.stampede;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.internal.Console;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.torodb.core.BuildProperties;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.metrics.MetricsConfig;
import com.torodb.engine.essential.DefaultBuildProperties;
import com.torodb.engine.essential.EssentialModule;
import com.torodb.packaging.config.model.backend.BackendPasswordConfig;
import com.torodb.packaging.config.model.backend.derby.AbstractDerby;
import com.torodb.packaging.config.model.backend.postgres.AbstractPostgres;
import com.torodb.packaging.config.model.protocol.mongo.AbstractReplication;
import com.torodb.packaging.config.model.protocol.mongo.MongoPasswordConfig;
import com.torodb.packaging.config.util.BackendImplementationVisitor;
import com.torodb.packaging.config.util.BundleFactory;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.util.Log4jUtils;
import com.torodb.stampede.config.model.Config;
import com.torodb.stampede.config.model.backend.Backend;
import com.torodb.stampede.config.model.mongo.replication.Replication;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Clock;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

/**
 * ToroDB Stampede entry point.
 */
public class Main {

  private static final Logger LOGGER = LogManager.getLogger(Main.class);

  /**
   * The main method that runs ToroDB Stampede.
   */
  public static void main(String[] args) throws Exception {
    try {
      Console console = JCommander.getConsole();

      ResourceBundle cliBundle = PropertyResourceBundle.getBundle("CliMessages");
      final CliConfig cliConfig = new CliConfig();
      @SuppressWarnings("checkstyle:LocalVariableName")
      JCommander jCommander = new JCommander(cliConfig, cliBundle, args);
      jCommander.setColumnSize(Integer.MAX_VALUE);

      if (cliConfig.isVersion()) {
        BuildProperties buildProperties = new DefaultBuildProperties();
        console.println(buildProperties.getFullVersion());
        System.exit(0);
      }

      if (cliConfig.isHelp()) {
        jCommander.usage();
        System.exit(0);
      }

      if (cliConfig.isHelpParam()) {
        console.println(cliBundle.getString("cli.help-param-header"));
        ConfigUtils.printParamDescriptionFromConfigSchema(Config.class, cliBundle, console, 0);
        System.exit(0);
      }

      cliConfig.addParams();

      final Config config = CliConfigUtils.readConfig(cliConfig);

      if (cliConfig.isPrintConfig()) {
        ConfigUtils.printYamlConfig(config, console);

        System.exit(0);
      }

      if (cliConfig.isPrintXmlConfig()) {
        ConfigUtils.printXmlConfig(config, console);

        System.exit(0);
      }

      if (cliConfig.hasPrintParams()) {
        StringBuilder printParamsBuilder = new StringBuilder();
        
        int index = 0;
        for (String printParamPath : cliConfig.getPrintParamPaths()) {
          JsonNode jsonNode = ConfigUtils.getParam(config, printParamPath);

          if (index++ > 0) {
            printParamsBuilder.append(",");
          }
          
          if (jsonNode != null) {
            printParamsBuilder.append(jsonNode.asText());
          }
        }
        
        console.print(printParamsBuilder.toString());

        System.exit(0);
      }

      configureLogger(cliConfig, config);

      parseToropassFile(config);

      AbstractReplication replication = config.getReplication();
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
            return config.getReplication().getMongopassFile();
          }

          @Override
          public String getHost() {
            return syncSource.getHost();
          }

          @Override
          public String getDatabase() {
            return replication.getAuth().getSource();
          }
        });
      }

      if (config.getBackend().isLike(AbstractPostgres.class)) {
        AbstractPostgres postgres = config.getBackend().as(AbstractPostgres.class);

        if (cliConfig.isAskForPassword()) {
          console.print("Type database user " + postgres.getUser() + "'s password:");
          postgres.setPassword(readPwd());
        }

        if (postgres.getPassword() == null) {
          throw new SystemException("No password provided for database user " + postgres.getUser()
              + ".\n\n"
              + "Please add following line to file " + postgres.getToropassFile() + ":\n"
              + postgres.getHost() + ":" + postgres.getPort() + ":"
              + postgres.getDatabase() + ":" + postgres.getUser() + ":<password>\n"
              + "Replace <password> for database user " + postgres.getUser() + "'s password");
        }
      }

      try {

        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
          @Override
          @SuppressFBWarnings(value = "DM_EXIT",
              justification =
              "Since is really hard to stop cleanly all threads when an OOME is thrown we must "
                  + "exit to avoid no more action is performed that could lead to an unespected "
                  + "state")
          public void uncaughtException(Thread thread, Throwable ex) {
            if (ex instanceof OutOfMemoryError) {
              try {
                LOGGER.error("Fatal out of memory: " + ex.getLocalizedMessage(), ex);
              } finally {
                System.exit(1);
              }
            }
          }
        });

        Service stampedeService = new StampedeService(createStampedeConfig(config));

        stampedeService.startAsync();
        stampedeService.awaitTerminated();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          stampedeService.stopAsync();
          stampedeService.awaitTerminated();
        }));
      } catch (CreationException ex) {
        ex.getErrorMessages().stream().forEach(m -> {
          if (m.getCause() != null) {
            LOGGER.error(m.getCause().getMessage());
          } else {
            LOGGER.error(m.getMessage());
          }
        });
        LogManager.shutdown();
        System.exit(1);
      }
    } catch (Throwable ex) {
      LOGGER.debug("Fatal error on initialization", ex);
      Throwable rootCause = Throwables.getRootCause(ex);
      String causeMessage = rootCause.getMessage() != null ? rootCause.getMessage() : 
          "internal error";
      LogManager.shutdown();
      JCommander.getConsole().println("Fatal error while ToroDB was starting: " + causeMessage);
      System.exit(1);
    }
  }

  private static StampedeConfig createStampedeConfig(Config config) {
    Clock clock = Clock.systemDefaultZone();

    MetricsConfig metricsConfig = config::getMetricsEnabled;
    Backend backendConfig = config.getBackend();
    Replication replicationConfig = config.getReplication();

    return new StampedeConfig(
        Guice.createInjector(new EssentialModule(metricsConfig, clock)),
        generalConfig -> BundleFactory.createBackendBundle(
            backendConfig,
            generalConfig
        ),
        generalConfig -> BundleFactory.createMongoDbReplConfigBundle(
            replicationConfig,
            generalConfig
        )
    );
  }

  private static void configureLogger(CliConfig cliConfig, Config config) {
    // If not specified in configuration then the log4j2.xml is used
    // instead (by default)
    if (config.getLogging().getLog4j2File() != null) {
      Log4jUtils.reconfigure(config.getLogging().getLog4j2File());
    }

    if (config.getLogging().getLevel() != null) {
      Log4jUtils.setRootLevel(config.getLogging().getLevel());
    }

    if (config.getLogging().getPackages() != null) {
      Log4jUtils.setLogPackages(config.getLogging().getPackages());
    }

    if (config.getLogging().getFile() != null) {
      Log4jUtils.appendToLogFile(config.getLogging().getFile());
    }
  }

  private static String readPwd() throws IOException {
    Console console = JCommander.getConsole();
    if (System.console() == null) { // In Eclipse IDE
      InputStream in = System.in;
      int max = 50;
      byte[] bytes = new byte[max];

      int length = in.read(bytes);
      length--;// last character is \n
      if (length > 0) {
        byte[] newBytes = new byte[length];
        System.arraycopy(bytes, 0, newBytes, 0, length);
        return new String(newBytes, Charsets.UTF_8);
      } else {
        return null;
      }
    } else { // Outside Eclipse IDE
      return new String(console.readPassword(false));
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
          ConfigUtils.parseToropassFile(value);
        } catch (Exception ex) {
          throw new SystemException(ex);
        }
      }
    };

    config.getBackend().getBackendImplementation().accept(visitor, null);
  }
}
