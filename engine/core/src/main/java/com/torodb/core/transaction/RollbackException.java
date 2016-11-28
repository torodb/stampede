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

package com.torodb.core.transaction;

import com.torodb.core.exceptions.ToroRuntimeException;

/**
 * This exception is thrown when something wrong happen but it is possible that it won't happen if
 * the transaction is executed again.
 *
 * As an example, when two sql connections try to write some changes on repeatable read isolation
 * level using optimistic locks, one of the two connections may be rollbacked with an exception, but
 * if it is repeated, it will probably work.
 */
public class RollbackException extends ToroRuntimeException {

  private static final long serialVersionUID = 8570701795687384298L;

  public RollbackException() {
  }

  public RollbackException(String message) {
    super(message);
  }

  public RollbackException(String message, Throwable cause) {
    super(message, cause);
  }

  public RollbackException(Throwable cause) {
    super(cause);
  }

}
