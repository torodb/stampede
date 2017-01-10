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

package com.torodb.core.modules;

import com.google.inject.Injector;
import com.torodb.core.Shutdowner;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.transaction.metainf.MetainfoRepository;

import java.util.concurrent.ThreadFactory;

public interface BundleConfig {
  /**
   * Retuns an {@link Injector} that contains essential implementations like {@link Retrier},
   * {@link Shutdowner} or {@link MetainfoRepository}.
   */
  public Injector getEssentialInjector();

  /**
   * Returns the thread factory the bundle will use to execute its start and stop methods.
   */
  public default ThreadFactory getThreadFactory() {
    return getEssentialInjector().getInstance(ThreadFactory.class);
  }

  /**
   * Returns the supervisor that will supervise the bundle.
   */
  public Supervisor getSupervisor();
}
