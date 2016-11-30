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

package com.torodb.core.retrier;

import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.transaction.RollbackException;

/**
 * This exception can be thrown by task executed by a {@link Retrier} to indicate that it should
 * give up.
 *
 * The retrier will abort when any runtime exception different than {@link RollbackException} is
 * thrown, so any runtime exception can be thrown, but this class is used to encourage to not throw
 * a plain RuntimeException in the case the callable actively wants to abort following executions.
 */
public class RetrierAbortException extends ToroRuntimeException {

  private static final long serialVersionUID = 5610815170219321703L;

  public RetrierAbortException(String message) {
    super(message);
  }

  public RetrierAbortException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetrierAbortException(Throwable cause) {
    super(cause);
  }

}
