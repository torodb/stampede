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


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.torodb.core.modules.Bundle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

/**
 * An abstract class used to simplify tests on the bundles used on this project.
 */
public abstract class AbstractReplBundleTest<B extends Bundle> {

  private List<Service> dependencies;

  /**
   * Returns the bundle that will be tested.
   *
   * <p/>The returned bundle <b>must not</b> be started. All methods annotated with
   * {@link Before @Before} will be executed before this method.
   */
  @Nonnull
  public abstract B getBundle();

  /**
   * Set the dependencies used by a test execution.
   * 
   * <p/>This dependencies will be stopped once the test is finalized (on a {@link After} phase).
   *
   * <p/>Dependencies will be started in the order specified by the list and stopped in the reverse
   * order.
   */
  protected void setDependencies(List<Service> dependencies) {
    this.dependencies = dependencies;
  }

  protected void startDependencies() {
    if (dependencies != null) {
      for (Service dependency : dependencies) {
        if (!dependency.isRunning()) {
          dependency.startAsync();
          dependency.awaitRunning();
        }
      }
    }
  }

  @After
  public void tearDown() {
    RuntimeException ex = null;

    if (dependencies != null) {
      for (Service dependency : Lists.reverse(dependencies)) {
        dependency.stopAsync();
        try {
          dependency.awaitTerminated(10, TimeUnit.SECONDS);
        } catch (TimeoutException ex2) {
          ex = new RuntimeException(ex2);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Test(timeout = 5_000L)
  public void testStartAndStop() {
    startDependencies();

    B bundle = getBundle();
    bundle.start()
        .thenCompose((o) -> bundle.stop())
        .join();
    assertThat(bundle.state(), is(Service.State.TERMINATED));
  }
}
