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

package com.torodb.engine.mongodb.sharding.isolation.db;

import com.google.common.util.concurrent.Service;
import com.torodb.core.modules.AbstractBundle;
import com.torodb.core.modules.BundleConfig;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodExtInt;
import com.torodb.torod.TorodServer;

import java.util.Collection;
import java.util.Collections;

public class DbIsolatedTorodBundle extends AbstractBundle<TorodExtInt> implements TorodBundle {

  private final TorodServer torodServer;

  public DbIsolatedTorodBundle(String shardId, TorodServer realServer, BundleConfig bundleConfig) {
    super(bundleConfig);
    this.torodServer = new DbIsolatorServer(shardId, realServer, bundleConfig.getThreadFactory());
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    torodServer.startAsync();
    torodServer.awaitRunning();
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    torodServer.stopAsync();
    torodServer.awaitTerminated();
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  public TorodExtInt getExternalInterface() {
    return () -> torodServer;
  }
}
