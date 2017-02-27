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

package com.torodb.torod;

import com.torodb.core.bundle.AbstractBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public abstract class AbstractTorodBundle extends AbstractBundle<TorodExtInt> 
    implements TorodBundle {

  private static final Logger LOGGER = LogManager.getLogger(AbstractTorodBundle.class);
  
  protected AbstractTorodBundle(BundleConfig config) {
    super(config);
  }

  /**
   * Returns the {@linkplain TorodServer} this bundle will use.
   *
   * <p>It must always return the same instance (or at least instances that share the service state)
   */
  protected abstract TorodServer getTorodServer();
  
  /**
   * Returns the {@linkplain ReservedIdGenerator} this bundle will use.
   * 
   * <p>It must always return the same instance (or at least instances that share the service state)
   */
  protected abstract ReservedIdGenerator getReservedIdGenerator();

  /**
   * Returns the {@linkplain InsertPipelineFactory} this bundle will use.
   *
   * <p>It must always return the same instance (or at least instances that share the service state)
   */
  protected abstract InsertPipelineFactory getInsertPipelineFactory();

  @Override
  protected void postDependenciesStartUp() throws Exception {
    ReservedIdGenerator reservedIdGenerator = getReservedIdGenerator();
    LOGGER.debug("Reading last used rids...");
    reservedIdGenerator.startAsync();
    reservedIdGenerator.awaitRunning();

    LOGGER.trace("Starting insert pipeline factories");
    InsertPipelineFactory insertPipelineFactory = getInsertPipelineFactory();
    insertPipelineFactory.startAsync();
    insertPipelineFactory.awaitRunning();

    LOGGER.debug("Starting Torod sevice");
    TorodServer torodServer = getTorodServer();
    torodServer.startAsync();
    torodServer.awaitRunning();
    LOGGER.debug("Torod sevice started");
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    TorodServer torodServer = getTorodServer();
    torodServer.stopAsync();
    torodServer.awaitTerminated();

    InsertPipelineFactory insertPipelineFactory = getInsertPipelineFactory();
    insertPipelineFactory.stopAsync();
    insertPipelineFactory.awaitTerminated();

    ReservedIdGenerator reservedIdGenerator = getReservedIdGenerator();
    reservedIdGenerator.stopAsync();
    reservedIdGenerator.awaitTerminated();
  }

  @Override
  public TorodExtInt getExternalInterface() {
    return () -> getTorodServer();
  }
}
