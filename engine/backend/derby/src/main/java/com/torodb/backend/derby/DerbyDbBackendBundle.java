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

package com.torodb.backend.derby;

import com.google.inject.Injector;
import com.torodb.backend.AbstractBackendBundle;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.derby.guice.DerbyBackendModule;
import com.torodb.backend.driver.derby.DerbyDbBackendConfig;
import com.torodb.core.backend.BackendExtInt;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.SnapshotUpdater;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;

/**
 * A {@link BackendBundle} that uses DerbyDB.
 */
public class DerbyDbBackendBundle extends AbstractBackendBundle {

  private final DbBackendService lowLevelService;
  private final BackendService backendService;
  private final ReservedIdGenerator reservedIdGenerator;
  private final SnapshotUpdater snapshotUpdater;
  private final IdentifierFactory identifierFactory;
  private final BackendTransactionJobFactory backendTransactionJobFactory;

  @SuppressWarnings("checkstyle:JavadocMethod")
  public DerbyDbBackendBundle(DerbyDbBackendConfig config) {
    super(config);
    Injector injector = config.getEssentialInjector().createChildInjector(
        new DerbyBackendModule(config));
    this.lowLevelService = injector.getInstance(DbBackendService.class);
    this.backendService = injector.getInstance(BackendService.class);
    this.reservedIdGenerator = injector.getInstance(ReservedIdGenerator.class);
    this.snapshotUpdater = injector.getInstance(SnapshotUpdater.class);
    this.identifierFactory = injector.getInstance(IdentifierFactory.class);
    this.backendTransactionJobFactory = injector.getInstance(BackendTransactionJobFactory.class);
  }

  @Override
  protected DbBackendService getLowLevelService() {
    return lowLevelService;
  }

  @Override
  protected BackendService getBackendService() {
    return backendService;
  }

  @Override
  public BackendExtInt getExternalInterface() {
    return new BackendExtInt() {
      @Override
      public BackendService getBackendService() {
        return backendService;
      }

      @Override
      public SnapshotUpdater getSnapshotUpdater() {
        return snapshotUpdater;
      }

      @Override
      public ReservedIdGenerator getReservedIdGenerator() {
        return reservedIdGenerator;
      }

      @Override
      public IdentifierFactory getIdentifierFactory() {
        return identifierFactory;
      }

      @Override
      public BackendTransactionJobFactory getBackendTransactionJobFactory() {
        return backendTransactionJobFactory;
      }
    };
  }

}
