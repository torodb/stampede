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

import com.torodb.core.logging.ComponentLoggerFactory;
import org.apache.logging.log4j.Logger;

public class BackendLoggerFactory extends ComponentLoggerFactory {

  private BackendLoggerFactory() {
    super("BACKEND");
  }

  public static BackendLoggerFactory getInstance() {
    return BackendLoggerFactoryHolder.INSTANCE;
  }

  public static Logger get(Class<?> clazz) {
    return getInstance().apply(clazz);
  }

  private static class BackendLoggerFactoryHolder {

    private static final BackendLoggerFactory INSTANCE = new BackendLoggerFactory();
  }

  //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return BackendLoggerFactory.getInstance();
  }
}
