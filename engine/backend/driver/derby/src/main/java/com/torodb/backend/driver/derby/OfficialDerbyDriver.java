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

package com.torodb.backend.driver.derby;

import com.google.common.base.Charsets;
import com.torodb.core.exceptions.SystemException;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

/**
 *
 * A provider for the Derby driver based on the "official" Derby driver
 *
 * @see <a href="https://db.apache.org/derby/docs/10.12/devguide/cdevdvlp40653.html">Derby JDBC
 * Driver</a>
 */
public class OfficialDerbyDriver implements DerbyDriverProvider {

  private static final Logger LOGGER = LogManager.getLogger(
      OfficialDerbyDriver.class
  );
  private static final Logger DRIVER_LOGGER = LogManager.getLogger(
      ClientDataSource.class
  );
  private static final PrintWriter LOGGER_WRITER = new PrintWriter(new LoggerWriter());
  public static final OutputStream LOGGER_OUTPUT = new OutputStream() {
    public void write(int b) {
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      LOGGER_WRITER.write(new String(b, off, len, Charsets.UTF_8));
    }
  };
  public static final OutputStream NO_OUTPUT = new OutputStream() {
    public void write(int b) {
    }
  };

  {
    if (DRIVER_LOGGER.isTraceEnabled()) {
      DriverManager.setLogWriter(LOGGER_WRITER);
      System.setProperty("derby.stream.error.field", OfficialDerbyDriver.class.getName()
          + ".LOGGER_OUTPUT");
    } else {
      System.setProperty("derby.stream.error.field", OfficialDerbyDriver.class.getName()
          + ".NO_OUTPUT");
    }
  }

  @Override
  public DataSource getConfiguredDataSource(DerbyDbBackendConfiguration configuration,
      String poolName) {
    DataSource dataSource;
    if (configuration.embedded()) {
      EmbeddedDataSource embeddedDataSource = new EmbeddedDataSource();
      embeddedDataSource.setCreateDatabase("create");
      if (configuration.inMemory()) {
        embeddedDataSource.setDatabaseName("memory:" + configuration.getDbName());
      } else {
        embeddedDataSource.setDatabaseName(configuration.getDbName());
      }

      try (Connection connection = embeddedDataSource.getConnection()) {
        LOGGER.debug("Derby test connection has been successfully created.");
      } catch (SQLException ex) {
        throw new SystemException(ex);
      }
      embeddedDataSource.setCreateDatabase(null);
      dataSource = embeddedDataSource;
    } else {
      ClientDataSource clientDataSource = new ClientDataSource();
      clientDataSource.setServerName(configuration.getDbHost());
      clientDataSource.setPortNumber(configuration.getDbPort());
      clientDataSource.setUser(configuration.getUsername());
      clientDataSource.setPassword(configuration.getPassword());
      if (configuration.inMemory()) {
        clientDataSource.setDatabaseName("memory:" + configuration.getDbName());
      } else {
        clientDataSource.setDatabaseName(configuration.getDbName());
      }
      dataSource = clientDataSource;
    }

    if (LOGGER.isTraceEnabled()) {
      try {
        dataSource.setLogWriter(LOGGER_WRITER);
      } catch (SQLException sqlException) {
        throw new SystemException(sqlException);
      }
    }

    //TODO
    try (Connection conn = dataSource.getConnection();
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1")) {
      rs.next();
    } catch (SQLException ex) {
      throw new SystemException(ex);
    }
    return dataSource;
  }

  private static class LoggerWriter extends Writer {

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      final StringBuilder messageBuilder = new StringBuilder();
      messageBuilder.append(cbuf, off, len);
      String message = messageBuilder.toString().replaceAll("(\r\n|\r|\n)$", "");
      if (!message.isEmpty()) {
        DRIVER_LOGGER.trace(message);
      }
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
  }
}
