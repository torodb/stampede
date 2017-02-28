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

package com.torodb.core.bundle;

import com.google.common.util.concurrent.Service;
import com.torodb.core.supervision.Supervisor;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * A bundle that wraps another one but transforms its external interface to another value.
 */
public class TransformationBundle<I, O> implements Bundle<O> {

  private final Bundle<I> delegate;
  private final Function<I, O> transformationFunction;

  public TransformationBundle(Bundle<I> delegate, Function<I, O> transformationFunction) {
    this.delegate = delegate;
    this.transformationFunction = transformationFunction;
  }

  @Override
  public Collection<Service> getDependencies() {
    return delegate.getDependencies();
  }

  @Override
  public O getExternalInterface() {
    return transformationFunction.apply(delegate.getExternalInterface());
  }

  @Override
  public Supervisor getSupervisor() {
    return delegate.getSupervisor();
  }

  @Override
  public Service startAsync() {
    return delegate.startAsync();
  }

  @Override
  public boolean isRunning() {
    return delegate.isRunning();
  }

  @Override
  public State state() {
    return delegate.state();
  }

  @Override
  public Service stopAsync() {
    return delegate.stopAsync();
  }

  @Override
  public void awaitRunning() {
    delegate.awaitRunning();
  }

  @Override
  public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {
    delegate.awaitRunning(timeout, unit);
  }

  @Override
  public void awaitTerminated() {
    delegate.awaitTerminated();
  }

  @Override
  public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
    delegate.awaitTerminated(timeout, unit);
  }

  @Override
  public Throwable failureCause() {
    return delegate.failureCause();
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    delegate.addListener(listener, executor);
  }

}
