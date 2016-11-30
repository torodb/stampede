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

package com.torodb.core;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.torodb.core.services.ExecutorTorodbService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 *
 */
@ThreadSafe
public class Shutdowner extends ExecutorTorodbService<ExecutorService> {

  private static final Logger LOGGER = LogManager.getLogger(Shutdowner.class);
  private boolean shuttingDown;

  @SuppressWarnings("rawtypes")
  private final List<ShutdownCallback> closeCallbacks = new ArrayList<>();

  @Inject
  @SuppressWarnings("checkstyle:Indentation")
  public Shutdowner(ThreadFactory threadFactory) {
    super(
        threadFactory,
        (ThreadFactory tf) -> {
      return Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
              .setThreadFactory(tf)
              .setNameFormat("torodb-shutdowner-%d")
              .build()
      );
    });
  }

  public CompletableFuture<Boolean> addCloseShutdownListener(
      AutoCloseable autoCloseable) {
    ShutdownCallback<AutoCloseable> callback =
        new AutoCloseableShutdownCallback(autoCloseable);
    return addShutdownCallback(callback);
  }

  public CompletableFuture<Boolean> addStopShutdownListener(Service service) {
    ShutdownCallback<Service> callback = new ServiceShutdownCallback(service);
    return addShutdownCallback(callback);
  }

  public <R> CompletableFuture<Boolean> addShutdownListener(
      R resource,
      ShutdownListener<R> shutdownListener) {
    ShutdownCallback<R> callback = new ShutdownListenerShutdownCallback<>(
        resource,
        shutdownListener);
    return addShutdownCallback(callback);
  }

  private <R> CompletableFuture<Boolean> addShutdownCallback(
      ShutdownCallback<R> callback) {
    return execute(() -> addShutdownCallbackPrivate(callback))
        .handle((result, throwable) -> {
          if (throwable != null) {
            return result;
          }
          if (throwable instanceof CancellationException) {
            executeCloseCallback(callback);
            return true;
          } else {
            if (throwable instanceof CompletionException) {
              throw (CompletionException) throwable;
            } else {
              throw new CompletionException(throwable);
            }
          }
        });
  }

  private <R> boolean addShutdownCallbackPrivate(ShutdownCallback<R> callback) {
    if (!shuttingDown) {
      closeCallbacks.add(callback);
      return true;
    } else {
      executeCloseCallback(callback);
      return false;
    }
  }

  @Override
  protected void shutDown() throws Exception {
    execute(this::closePrivate).join();
    super.shutDown();
  }

  @SuppressWarnings("rawtypes")
  private void closePrivate() {
    shuttingDown = true;
    Lists.reverse(closeCallbacks).forEach(closeCallback -> {
      if (closeCallback != null) {
        executeCloseCallback(closeCallback);
      }
    });
  }

  private void executeCloseCallback(ShutdownCallback<?> callback) {
    try {
      LOGGER.debug("Shutting down {}", callback::describeResource);
      callback.onShutdown();
    } catch (Throwable t) {
      LOGGER.error("Error while trying to shutdown the resource "
          + callback.getResource(), t);
    }
  }

  public static interface ShutdownListener<E> {

    public void onShutdown(E e) throws Exception;

    public default String describeResource(E resource) {
      return resource.toString();
    }
  }

  public abstract static class ShutdownCallback<E> {

    private final E resource;

    public ShutdownCallback(E resource) {
      this.resource = resource;
    }

    public abstract String describeResource();

    public E getResource() {
      return resource;
    }

    public abstract void onShutdown() throws Exception;
  }

  private static class ShutdownListenerShutdownCallback<E> extends ShutdownCallback<E> {

    private final ShutdownListener<E> listener;

    public ShutdownListenerShutdownCallback(E resource,
        ShutdownListener<E> listener) {
      super(resource);
      this.listener = listener;
    }

    @Override
    public String describeResource() {
      return listener.describeResource(getResource());
    }

    @Override
    public void onShutdown() throws Exception {
      listener.onShutdown(getResource());
    }
  }

  private static class ServiceShutdownCallback extends ShutdownCallback<Service> {

    public ServiceShutdownCallback(Service resource) {
      super(resource);
    }

    @Override
    public String describeResource() {
      return getResource() + " service";
    }

    @Override
    public void onShutdown() throws Exception {
      getResource().stopAsync();
      try {
        getResource().awaitTerminated();
      } catch (IllegalStateException ex) {
        LOGGER.warn(getResource() + " failed before it can be stopped", ex);
      }
    }
  }

  private static class AutoCloseableShutdownCallback extends ShutdownCallback<AutoCloseable> {

    public AutoCloseableShutdownCallback(AutoCloseable resource) {
      super(resource);
    }

    @Override
    public String describeResource() {
      return getResource() + " autocloseable";
    }

    @Override
    public void onShutdown() throws Exception {
      getResource().close();
    }
  }

}
