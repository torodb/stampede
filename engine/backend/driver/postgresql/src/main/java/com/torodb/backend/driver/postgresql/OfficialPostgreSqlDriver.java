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

package com.torodb.backend.driver.postgresql;

import com.torodb.core.exceptions.SystemException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.Driver;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
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
 * A provider for the PostgreSQL driver based on the "official" PostgreSQL driver
 *
 * @see <a href="http://jdbc.postgresql.org/">PostgreSQL JDBC Driver</a>
 */
public class OfficialPostgreSqlDriver implements PostgreSqlDriverProvider {

  private static final Logger JDBC_LOGGER = LogManager.getLogger(
      PGSimpleDataSource.class
  );
  private static final PrintWriter LOGGER_WRITER = new PrintWriter(new LoggerWriter());

  {
    if (JDBC_LOGGER.isTraceEnabled()) {
      DriverManager.setLogWriter(LOGGER_WRITER);
    }
  }

  @Override
  public DataSource getConfiguredDataSource(PostgreSqlBackendConfiguration configuration,
      String poolName) {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();

    dataSource.setUser(configuration.getUsername());
    dataSource.setPassword(configuration.getPassword());
    dataSource.setServerName(configuration.getDbHost());
    dataSource.setPortNumber(configuration.getDbPort());
    dataSource.setDatabaseName(configuration.getDbName());

    dataSource.setApplicationName("torodb-" + poolName);

    if (JDBC_LOGGER.isTraceEnabled()) {
      dataSource.setLogLevel(Driver.DEBUG);
      dataSource.setLogWriter(LOGGER_WRITER);
    }

    //TODO
    
    try (
        Connection conn = dataSource.getConnection();
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT 1")) {
      rs.next();
    } catch (SQLException ex) {
      throw new SystemException(ex.getLocalizedMessage());
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
        JDBC_LOGGER.trace(message);
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
