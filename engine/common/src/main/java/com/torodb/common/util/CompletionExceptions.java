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

package com.torodb.common.util;

import java.util.concurrent.CompletionException;

/**
 * CompletionException utility class.
 */
public class CompletionExceptions {

  private CompletionExceptions() {
  }

  /**
   * Returns the first cause of the given exception that is not a {@link CompletionException} or the
   * deepest CompletionException if there are no exception different than CompletionException in the
   * stack.
   *
   * @param ex the original exception.
   * @return The first exception that is not CompletionException or the given exception.
   */
  public static Throwable getFirstNonCompletionException(Throwable ex) {
    Throwable throwableResult = ex;
    while (isCompletionException(throwableResult)) {
      Throwable cause = throwableResult.getCause();
      if (cause == null) {
        return throwableResult;
      }
      throwableResult = cause;
    }
    assert throwableResult != null;
    assert !(throwableResult instanceof CompletionException) || throwableResult.getCause() == null;
    return throwableResult;
  }

  private static boolean isCompletionException(Throwable throwableResult) {
    return throwableResult instanceof CompletionException
            && throwableResult != throwableResult.getCause()
            && throwableResult.getCause() != null;
  }

}
