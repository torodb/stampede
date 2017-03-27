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

package com.torodb.core.bundle;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A utility bundle that wraps a resource which depends on some services managed by this class.
 */
public abstract class DependenciesBundle<ExtIntT> extends AbstractBundle<ExtIntT> {

  protected abstract List<Service> getManagedDependencies();

  public DependenciesBundle(BundleConfig bundleConfig) {
    super(bundleConfig);
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    for (Service managedDependency : getManagedDependencies()) {
      managedDependency.startAsync();
      managedDependency.awaitRunning();
    }
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    for (Service managedDependency : Lists.reverse(getManagedDependencies())) {
      managedDependency.stopAsync();
      managedDependency.awaitTerminated();
    }
  }
}
