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

package com.torodb.mongodb.repl.impl;

import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.supervision.Supervisor;

public class FollowerSyncSourceProviderConfig implements BundleConfig {
  private final HostAndPort seed;
  private final BundleConfig delegate;

  public FollowerSyncSourceProviderConfig(HostAndPort seed, BundleConfig delegate) {
    this.seed = seed;
    this.delegate = delegate;
  }

  public HostAndPort getSeed() {
    return seed;
  }

  @Override
  public Injector getEssentialInjector() {
    return delegate.getEssentialInjector();
  }

  @Override
  public Supervisor getSupervisor() {
    return delegate.getSupervisor();
  }
}
