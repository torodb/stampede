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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is like {@link Void} but it has one instance.
 * <p/>
 * This can be useful when you want an implementation of a generic class that returns non null
 * instances of the generic type. For instance, commands that do not need argument or do not return
 * information, can use this class.
 */
public class Empty {

  private Empty() {
  }

  public static Empty getInstance() {
    return EmptyHolder.INSTANCE;
  }

  private static class EmptyHolder {

    private static final Empty INSTANCE = new Empty();
  }

  @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return Empty.getInstance();
  }
}
