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

import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendExtInt;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.modules.AbstractBundle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractBackendBundle extends AbstractBundle<BackendExtInt>
    implements BackendBundle {
  private static final Logger LOGGER = LogManager.getLogger(AbstractBackendBundle.class);

  public AbstractBackendBundle(BackendConfig bundleConfig) {
    super(bundleConfig);
  }

  /**
   * Returns the low level backend service used by this bundle.
   *
   * <p>It must always return the same instance.
   */
  protected abstract DbBackendService getLowLevelService();

  /**
   * Returns the backend service used by this bundle.
   *
   * <p>It must always return the same instance.
   */
  protected abstract BackendService getBackendService();

  @Override
  protected void postDependenciesStartUp() throws Exception {
    LOGGER.debug("Starting low level backend service");
    DbBackendService lowLevelService = getLowLevelService();
    lowLevelService.startAsync();
    lowLevelService.awaitRunning();
    LOGGER.debug("Low level backend service started");

    LOGGER.debug("Starting backend service");
    BackendService backendService = getBackendService();
    backendService.startAsync();
    backendService.awaitRunning();
    LOGGER.debug("Backend service started");

    LOGGER.debug("Validating database metadata");
    try (BackendConnection conn = backendService.openConnection();
        ExclusiveWriteBackendTransaction trans = conn.openExclusiveWriteTransaction()) {

      trans.checkOrCreateMetaDataTables();
      trans.commit();
    }
    LOGGER.info("Database metadata has been validated");
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    LOGGER.debug("Shutting down backend service");
    BackendService backendService = getBackendService();
    backendService.stopAsync();
    backendService.awaitTerminated();
    LOGGER.debug("Backend service shutted down");

    LOGGER.debug("Shutting down backend low level service");
    DbBackendService lowLevelService = getLowLevelService();
    lowLevelService.stopAsync();
    lowLevelService.awaitTerminated();
    LOGGER.debug("Low level backend service shutted down");
  }

}
