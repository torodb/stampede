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

package com.torodb.core.d2r;

import com.torodb.core.logging.ComponentLoggerFactory;
import org.apache.logging.log4j.Logger;

public class D2RLoggerFactory extends ComponentLoggerFactory {

  private D2RLoggerFactory() {
    super("D2R");
  }

  public static D2RLoggerFactory getInstance() {
    return D2RLoggerFactoryHolder.INSTANCE;
  }

  public static Logger get(Class<?> clazz) {
    return getInstance().apply(clazz);
  }

  private static class D2RLoggerFactoryHolder {

    private static final D2RLoggerFactory INSTANCE = new D2RLoggerFactory();
  }

  //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return D2RLoggerFactory.getInstance();
  }
}
