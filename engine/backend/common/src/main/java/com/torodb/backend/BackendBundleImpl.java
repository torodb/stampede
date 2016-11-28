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

import com.google.inject.assistedinject.Assisted;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.modules.AbstractBundle;
import com.torodb.core.supervision.Supervisor;

import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;

/**
 *
 */
public class BackendBundleImpl extends AbstractBundle implements BackendBundle {

  private final DbBackendService lowLevelService;
  private final BackendService backendService;

  @Inject
  public BackendBundleImpl(DbBackendService lowLevelService,
      BackendServiceImpl backendService, ThreadFactory threadFactory,
      @Assisted Supervisor supervisor) {
    super(threadFactory, supervisor);
    this.lowLevelService = lowLevelService;
    this.backendService = backendService;
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    lowLevelService.startAsync();
    lowLevelService.awaitRunning();

    backendService.startAsync();
    backendService.awaitRunning();

    try (BackendConnection conn = backendService.openConnection();
        ExclusiveWriteBackendTransaction trans = conn.openExclusiveWriteTransaction()) {

      trans.checkOrCreateMetaDataTables();
      trans.commit();
    }
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    backendService.stopAsync();
    backendService.awaitTerminated();

    lowLevelService.stopAsync();
    lowLevelService.awaitTerminated();
  }

  @Override
  public BackendService getBackendService() {
    return backendService;
  }

}
