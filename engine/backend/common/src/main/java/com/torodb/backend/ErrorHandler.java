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

package com.torodb.backend;

import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import org.jooq.exception.DataAccessException;

import java.sql.SQLException;

public interface ErrorHandler {

  /**
   * The context of the backend error that reflect the specific operation that is performed when an
   * error is received.
   */
  public enum Context {
    UNKNOWN,
    GET_CONNECTION,
    CREATE_SCHEMA,
    CREATE_TABLE,
    ADD_COLUMN,
    CREATE_INDEX,
    ADD_UNIQUE_INDEX,
    ADD_FOREIGN_KEY,
    DROP_SCHEMA,
    DROP_TABLE,
    RENAME_TABLE,
    RENAME_INDEX,
    SET_TABLE_SCHEMA,
    DROP_INDEX,
    DROP_UNIQUE_INDEX,
    DROP_FOREIGN_KEY,
    FETCH,
    META_INSERT,
    META_DELETE,
    INSERT,
    UPDATE,
    DELETE,
    COMMIT,
    ROLLBACK,
    CLOSE
  }

  /**
   * Return the unchecked ToroRuntimeException exception that must be thrown.
   *
   * @param context
   * @param sqlException
   * @return an unchecked ToroRuntimeException
   * @throws RollbackException if the {@code sqlException} is due to a conflict resolvable by
   *                           repeating the operation
   */
  ToroRuntimeException handleException(Context context, SQLException sqlException) throws
      RollbackException;

  /**
   * Return the unchecked ToroRuntimeException exception that must be thrown.
   *
   * @param context
   * @param dataAccessException
   * @return an unchecked ToroRuntimeException
   * @throws RollbackException if the {@code dataAccessException} is due to a conflict resolvable by
   *                           repeating the operation
   */
  ToroRuntimeException handleException(Context context, DataAccessException dataAccessException)
      throws RollbackException;

  /**
   * Return the unchecked ToroRuntimeException exception that must be thrown.
   *
   * @param context
   * @param sqlException
   * @return
   * @throws UserException     if the {@code sqlException} is due to an user mistake
   * @throws RollbackException if the {@code sqlException} is due to a conflict resolvable by
   *                           repeating the operation
   */
  ToroRuntimeException handleUserException(Context context, SQLException sqlException) throws
      UserException, RollbackException;

  /**
   * Return the unchecked ToroRuntimeException exception that must be thrown.
   *
   * @param context
   * @param dataAccessException
   * @return
   * @throws UserException     if the {@code dataAccessException} is due to an user mistake
   * @throws RollbackException if the {@code dataAccessException} is due to a conflict resolvable by
   *                           repeating the operation
   */
  ToroRuntimeException handleUserException(Context context, DataAccessException dataAccessException)
      throws UserException, RollbackException;
}
