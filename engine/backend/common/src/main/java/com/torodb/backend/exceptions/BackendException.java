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

package com.torodb.backend.exceptions;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.exceptions.SystemException;
import org.jooq.exception.DataAccessException;

import java.sql.SQLException;

/**
 *
 */
public class BackendException extends SystemException {

  private static final long serialVersionUID = 1L;

  private final Context context;
  private final String sqlState;

  private static String sqlMessage(SQLException cause) {
    while (cause.getNextException() != null) {
      cause = cause.getNextException();
    }

    return cause.getMessage();
  }

  private static String sqlMessage(DataAccessException cause) {
    Throwable causeThroawle = cause.getCause();
    if (causeThroawle instanceof SQLException) {
      return sqlMessage((SQLException) causeThroawle);
    }
    return cause.getMessage();
  }

  public BackendException(Context context, SQLException cause) {
    super(sqlMessage(cause), cause);

    this.context = context;
    this.sqlState = cause.getSQLState();
  }

  public BackendException(Context context, DataAccessException cause) {
    super(sqlMessage(cause), cause);

    this.context = context;
    this.sqlState = cause.sqlState();
  }

  public Context getContext() {
    return context;
  }

  public String getSqlState() {
    return sqlState;
  }

  @Override
  public String getMessage() {
    return "On context " + context + " with sqlState " + sqlState + ": " + getCause().getMessage();
  }
}
