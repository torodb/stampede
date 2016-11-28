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

import com.torodb.core.exceptions.ToroException;

/**
 * This exception is thrown by {@link Retrier} when it found an error on the task to execut and it
 * decides to give up and do not retry it again.
 */
public class RetrierGiveUpException extends ToroException {

  private static final long serialVersionUID = -9052439274011714487L;

  public RetrierGiveUpException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetrierGiveUpException(Throwable cause) {
    super(cause);
  }

}
