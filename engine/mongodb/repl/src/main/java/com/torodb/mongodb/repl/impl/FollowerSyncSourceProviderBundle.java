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

import com.google.common.util.concurrent.Service;
import com.torodb.core.bundle.AbstractBundle;
import com.torodb.mongodb.repl.SyncSourceProvider;

import java.util.Collection;
import java.util.Collections;

public class FollowerSyncSourceProviderBundle extends AbstractBundle<SyncSourceProvider> {

  private final FollowerSyncSourceProvider syncSourceProvider;

  public FollowerSyncSourceProviderBundle(FollowerSyncSourceProviderConfig config) {
    super(config);
    this.syncSourceProvider = new FollowerSyncSourceProvider(config.getSeed());
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  public SyncSourceProvider getExternalInterface() {
    return syncSourceProvider;
  }

}
