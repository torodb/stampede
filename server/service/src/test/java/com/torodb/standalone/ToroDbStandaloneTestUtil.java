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

package com.torodb.standalone;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.core.Shutdowner;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.standalone.config.model.Config;
import com.torodb.torod.TorodBundle;

import java.time.Clock;

public class ToroDbStandaloneTestUtil {

  private static final Supervisor STOP_AND_THROW_SUPERVISOR = new Supervisor() {
    @Override
    public SupervisorDecision onError(Object supervised, Throwable error) {
      return SupervisorDecision.STOP;
    }
  };

  public static TestService createInjectors(Config config, Clock clock) {
    return new TestService(config, clock, STOP_AND_THROW_SUPERVISOR);
  }

  public static class TestService {

    private final BackendBundle backendBundle;
    private final Injector injector;
    private final TorodBundle torodBundle;

    private TestService(Config config, Clock clock, Supervisor supervisor) {
      Injector bootstrapInjector = Guice.createInjector(new BootstrapModule(
          config, clock));
      backendBundle = bootstrapInjector.getInstance(BackendBundleFactory.class)
          .createBundle(supervisor);
      ToroDbRuntimeModule runtimeModule = new ToroDbRuntimeModule(
          backendBundle, supervisor);
      injector = bootstrapInjector.createChildInjector(runtimeModule);
      torodBundle = injector.getInstance(TorodBundle.class);
    }

    public Injector getInjector() {
      return injector;
    }

    public void startBackendBundle() {
      backendBundle.startAsync();
      backendBundle.awaitRunning();
    }

    public void checkOrCreateMetaDataTables() {
      try (BackendConnection conn = injector.getInstance(BackendService.class).openConnection();
          ExclusiveWriteBackendTransaction trans = conn.openExclusiveWriteTransaction()) {

        trans.checkOrCreateMetaDataTables();
        trans.commit();
      } catch (UserException userException) {
        throw new RuntimeException(userException);
      }
    }

    public void startTorodBundle() {
      torodBundle.startAsync();
      torodBundle.awaitRunning();
    }

    public void shutDown() {
      Shutdowner shutdowner = injector.getInstance(Shutdowner.class);
      shutdowner.stopAsync();
      shutdowner.awaitTerminated();
    }
  }
}
