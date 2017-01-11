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

package com.torodb.mongodb.repl;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.engine.essential.EssentialModule;

import java.time.Clock;

/**
 * A {@link BundleConfig} used on tests.
 */
public class TestBundleConfig implements BundleConfig {

  private final Supervisor supervisor = new Supervisor() {
    @Override
    public SupervisorDecision onError(Object supervised, Throwable error) {
      throw new AssertionError("Error on " + supervised, error);
    }
  };

  private final Injector essentialInjector = Guice.createInjector(
      Stage.PRODUCTION,
      new EssentialModule(
          () -> true,
          Clock.systemUTC()
      )
  );

  @Override
  public Injector getEssentialInjector() {
    return essentialInjector;
  }

  @Override
  public Supervisor getSupervisor() {
    return supervisor;
  }

}
