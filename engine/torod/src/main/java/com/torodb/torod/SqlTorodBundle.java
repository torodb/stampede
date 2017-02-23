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

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.SnapshotUpdater;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.torod.guice.SqlTorodModule;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link TorodBundle torod bundle} that uses a SQL backend.
 */
public class SqlTorodBundle extends AbstractTorodBundle {
  private static final Logger LOGGER = LogManager.getLogger(SqlTorodBundle.class);
  private final TorodServer torodServer;
  private final BackendBundle backendBundle;
  private final SnapshotUpdater snapshotUpdater;
  private final ReservedIdGenerator reservedIdGenerator;
  private final InsertPipelineFactory insertPipelineFactory;
  private final MetainfoRepository metainfoRepository;

  public SqlTorodBundle(SqlTorodConfig config) {
    super(config);
    Injector injector = config.getEssentialInjector().createChildInjector(
        new SqlTorodModule(config));
    this.torodServer = injector.getInstance(TorodServer.class);
    this.insertPipelineFactory = injector.getInstance(InsertPipelineFactory.class);
    this.backendBundle = config.getBackendBundle();
    this.reservedIdGenerator = backendBundle.getExternalInterface().getReservedIdGenerator();
    this.snapshotUpdater = backendBundle.getExternalInterface().getSnapshotUpdater();
    this.metainfoRepository = injector.getInstance(MetainfoRepository.class);
  }

  @Override
  protected TorodServer getTorodServer() {
    return torodServer;
  }

  @Override
  protected ReservedIdGenerator getReservedIdGenerator() {
    return reservedIdGenerator;
  }

  @Override
  protected InsertPipelineFactory getInsertPipelineFactory() {
    return insertPipelineFactory;
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    LOGGER.trace("Loading backend metadata...");
    snapshotUpdater.updateSnapshot(metainfoRepository);

    super.postDependenciesStartUp();
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.singleton(backendBundle);
  }
}
