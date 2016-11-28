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

package com.torodb.core.modules;

import com.google.common.base.Preconditions;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.supervision.Supervisor;

import java.util.concurrent.ThreadFactory;

/**
 *
 */
public abstract class AbstractBundle extends IdleTorodbService implements Bundle {

  private final Supervisor supervisor;

  public AbstractBundle(ThreadFactory threadFactory, Supervisor supervisor) {
    super(threadFactory);
    this.supervisor = supervisor;
  }

  protected abstract void postDependenciesStartUp() throws Exception;

  protected abstract void preDependenciesShutDown() throws Exception;

  @Override
  protected final void startUp() throws Exception {
    awaitForDependencies();
    postDependenciesStartUp();
  }

  @Override
  protected final void shutDown() throws Exception {
    assertDependenciesRunning();
    preDependenciesShutDown();
  }

  private void awaitForDependencies() {
    getDependencies().forEach(d -> d.awaitRunning());
  }

  private void assertDependenciesRunning() {
    getDependencies().forEach(d ->
        Preconditions.checkState(
            d.isRunning(),
            "%s cannot be shutted down while %s is not running",
            this,
            d)
    );
  }

  @Override
  public Supervisor getSupervisor() {
    return supervisor;
  }

}
