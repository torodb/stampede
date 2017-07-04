/*
 * ToroDB Stampede
 * Copyright © 2016 8Kdata Technology (www.8kdata.com)
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
package com.torodb.stampede;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.backend.derby.DerbyDbBackendBundle;
import com.torodb.backend.derby.driver.DerbyDbBackendConfigBuilder;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.guice.EssentialModule;
import com.torodb.core.logging.DefaultLoggerFactory;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.mongodb.repl.oplogreplier.offheapbuffer.OffHeapBufferConfig;
import com.torodb.mongodb.repl.oplogreplier.offheapbuffer.BufferRollCycle;
import com.torodb.mongodb.repl.sharding.MongoDbShardingConfig;
import com.torodb.mongowp.client.wrapper.MongoClientConfigurationProperties;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

public class StampedeServiceTest {

  private StampedeConfig stampedeConfig;

  @SuppressWarnings("checkstyle:JavadocMethod")
  @Before
  public void setUp() {
    stampedeConfig = StampedeConfig.createShardingConfig(
        createEssentialInjector(),
        this::createBackendBundle,
        ReplicationFilters.allowAll(),
        createShards(1),
        DefaultLoggerFactory.getInstance(),
        createOffHeapBufferConfig()
    );
  }

  @Test
  public void testCreateStampedeService() {
    Service stampedeService = new StampedeService(stampedeConfig);
    assert !stampedeService.isRunning();
  }

  @Test
  @Ignore
  public void testCreateStampedeService_run() {
    Service stampedeService = new StampedeService(stampedeConfig);
    stampedeService.startAsync();
    stampedeService.awaitRunning();

    stampedeService.stopAsync();
    stampedeService.awaitTerminated();
  }

  private Injector createEssentialInjector() {
    return Guice.createInjector(new EssentialModule(
        DefaultLoggerFactory.getInstance(),
        () -> true,
        Clock.systemUTC())
    );
  }

  private BackendBundle createBackendBundle(BundleConfig bundleConfig) {
    return new DerbyDbBackendBundle(new DerbyDbBackendConfigBuilder(bundleConfig)
        .build()
    );
  }

  private List<StampedeConfig.ShardConfigBuilder> createShards(int size) {
    List<StampedeConfig.ShardConfigBuilder> result = new ArrayList<>(size);

    for (int i = 0; i < size; i++) {
      final int id = i;
      final MongoClientConfigurationProperties mccp = MongoClientConfigurationProperties.unsecure();

      StampedeConfig.ShardConfigBuilder scb = new StampedeConfig.ShardConfigBuilder() {
        @Override
        public String getShardId() {
          return "shard_" + id;
        }

        @Override
        public MongoDbShardingConfig.ShardConfig createConfig(
            ConsistencyHandler consistencyHandler) {
          return new MongoDbShardingConfig.ShardConfig(
              getShardId(),
              ImmutableList.of(HostAndPort.fromParts("localhost", 27017 + id)),
              mccp,
              "replSet",
              consistencyHandler
          );
        }
      };

      result.add(scb);
    }
    return result;
  }

  private OffHeapBufferConfig createOffHeapBufferConfig() {
    OffHeapBufferConfig bufferConfig = new OffHeapBufferConfig() {
      @Override
      public Boolean getEnabled() {
        return true;
      }

      @Override
      public String getPath() {
        return "";
      }

      @Override
      public int getMaxFiles() {
        return 5;
      }

      @Override
      public BufferRollCycle getRollCycle() {
        return BufferRollCycle.HOURLY;
      }
    };
    return bufferConfig;
  }
}
