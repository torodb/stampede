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

package com.torodb.packaging.guice;

import com.eightkdata.mongowp.annotations.MongoWp;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.torodb.concurrent.DefaultConcurrentToolsFactory;
import com.torodb.concurrent.DefaultConcurrentToolsFactory.BlockerThreadFactoryFunction;
import com.torodb.concurrent.DefaultConcurrentToolsFactory.ForkJoinThreadFactoryFunction;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.annotations.TorodbRunnableService;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class ExecutorServicesModule extends AbstractModule {

  @Override
  protected void configure() {

    bind(Integer.class)
        .annotatedWith(ParallelLevel.class)
        .toInstance(Runtime.getRuntime().availableProcessors());

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("torodb-%d")
        .build();

    bind(ThreadFactory.class)
        .toInstance(threadFactory);

    bind(ThreadFactory.class)
        .annotatedWith(TorodbIdleService.class)
        .toInstance(threadFactory);

    bind(ThreadFactory.class)
        .annotatedWith(TorodbRunnableService.class)
        .toInstance(threadFactory);

    bind(ThreadFactory.class)
        .annotatedWith(MongoWp.class)
        .toInstance(threadFactory);

    bind(ForkJoinWorkerThreadFactory.class)
        .toInstance(ForkJoinPool.defaultForkJoinWorkerThreadFactory);

    bind(DefaultConcurrentToolsFactory.BlockerThreadFactoryFunction.class)
        .toInstance(new CustomBlockerThreadFactoryFunction());

    bind(DefaultConcurrentToolsFactory.ForkJoinThreadFactoryFunction.class)
        .toInstance(new CustomForkJoinThreadFactoryFunction());
  }

  private static class CustomBlockerThreadFactoryFunction implements BlockerThreadFactoryFunction {

    @Override
    public ThreadFactory apply(String prefix) {
      return new ThreadFactoryBuilder()
          .setNameFormat(prefix + "-%d")
          .build();
    }
  }

  private static class CustomForkJoinThreadFactoryFunction
      implements ForkJoinThreadFactoryFunction {

    @Override
    public ForkJoinWorkerThreadFactory apply(String prefix) {
      return new CustomForkJoinThreadFactory(prefix);
    }
  }

  private static class CustomForkJoinThreadFactory implements ForkJoinWorkerThreadFactory {

    private final String prefix;
    private volatile int idProvider = 0;

    public CustomForkJoinThreadFactory(String prefix) {
      super();
      this.prefix = prefix;
    }

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
      ForkJoinWorkerThread newThread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(
          pool);
      int id = idProvider++;
      newThread.setName(prefix + '-' + id);
      return newThread;
    }
  }

}
