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

package com.torodb.stampede;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Injector;
import com.torodb.core.Shutdowner;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.modules.Bundle;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.modules.BundleConfigImpl;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.engine.mongodb.sharding.MongoDbShardingBundle;
import com.torodb.engine.mongodb.sharding.MongoDbShardingConfig;
import com.torodb.engine.mongodb.sharding.MongoDbShardingConfigBuilder;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.torod.SqlTorodBundle;
import com.torodb.torod.SqlTorodConfig;
import com.torodb.torod.TorodBundle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * This service is used to start and stop ToroDB Stampede.
 *
 * <p>It takes a {@link StampedeConfig} and uses it to create and start the required
 * {@link Bundle bundles}.
 */
public class StampedeService extends AbstractIdleService implements Supervisor {

  private static final Logger LOGGER = LogManager.getLogger(StampedeService.class);
  private final ThreadFactory threadFactory;
  private final StampedeConfig stampedeConfig;
  private final Injector essentialInjector;
  private final BundleConfig generalBundleConfig;
  private final Shutdowner shutdowner;

  public StampedeService(StampedeConfig stampedeConfig) {
    this.stampedeConfig = stampedeConfig;

    this.essentialInjector = stampedeConfig.getEssentialInjector();
    this.threadFactory = essentialInjector.getInstance(ThreadFactory.class);
    this.generalBundleConfig = new BundleConfigImpl(essentialInjector, this);
    this.shutdowner = essentialInjector.getInstance(Shutdowner.class);
  }

  @Override
  protected Executor executor() {
    return (Runnable command) -> {
      Thread thread = threadFactory.newThread(command);
      thread.start();
    };
  }

  @Override
  public SupervisorDecision onError(Object supervised, Throwable error) {
    LOGGER.error("Error reported by " + supervised + ". Stopping ToroDB Stampede", error);
    this.stopAsync();
    return SupervisorDecision.IGNORE;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting up ToroDB Stampede");

    shutdowner.startAsync();
    shutdowner.awaitRunning();
    
    BackendBundle backendBundle = stampedeConfig.getBackendBundleGenerator()
        .apply(generalBundleConfig);
    startBundle(backendBundle);

    Map<String, ConsistencyHandler> consistencyHandlers = createConsistencyHandlers(
        backendBundle,
        stampedeConfig.getThreadFactory()
    );

    resolveInconsistencies(backendBundle, consistencyHandlers);

    TorodBundle torodBundle = createTorodBundle(backendBundle);
    startBundle(torodBundle);

    MongoDbShardingBundle shardingBundle = createShardingBundle(torodBundle, consistencyHandlers);
    startBundle(shardingBundle);

    LOGGER.info("ToroDB Stampede is now running");
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Shutting down ToroDB Stampede");
    if (shutdowner != null) {
      shutdowner.stopAsync();
      shutdowner.awaitTerminated();
    }
    LOGGER.info("ToroDB Stampede has been shutted down");
  }

  private Map<String, ConsistencyHandler> createConsistencyHandlers(BackendBundle backendBundle,
      ThreadFactory threadFactory) {
    Retrier retrier = essentialInjector.getInstance(Retrier.class);
    BackendService backendService = backendBundle.getExternalInterface().getBackendService();

    Map<String, ConsistencyHandler> result = new HashMap<>();

    stampedeConfig.getShardConfigBuilders().stream()
        .map(StampedeConfig.ShardConfigBuilder::getShardId)
        .forEachOrdered((shardId) -> {
          ConsistencyHandler consistencyHandler = new ShardConsistencyHandler(
              shardId, backendService, retrier, threadFactory
          );

          consistencyHandler.startAsync();
          consistencyHandler.awaitRunning();
          
          result.put(shardId, consistencyHandler);
        });

    return result;
  }

  private TorodBundle createTorodBundle(BackendBundle backendBundle) {
    return new SqlTorodBundle(new SqlTorodConfig(
        backendBundle,
        essentialInjector,
        this
    ));
  }

  private MongoDbShardingBundle createShardingBundle(TorodBundle torodBundle,
      Map<String, ConsistencyHandler> consistencyHandler) {

    @SuppressWarnings("checkstyle:LineLength")
    MongoDbShardingConfigBuilder configBuilder = new MongoDbShardingConfigBuilder(generalBundleConfig)
        .setTorodBundle(torodBundle)
        .setUserReplFilter(stampedeConfig.getUserReplicationFilters());

    stampedeConfig.getShardConfigBuilders().forEach(shardConfBuilder ->
        addShard(configBuilder, shardConfBuilder, consistencyHandler)
    );

    return new MongoDbShardingBundle(configBuilder.build());
  }

  private void addShard(
      MongoDbShardingConfigBuilder shardingConfBuilder,
      StampedeConfig.ShardConfigBuilder shardConfBuilder,
      Map<String, ConsistencyHandler> consistencyHandlers) {

    ConsistencyHandler consistencyHandler = consistencyHandlers.get(shardConfBuilder.getShardId());

    MongoDbShardingConfig.ShardConfig shardConfig = shardConfBuilder.createConfig(
        consistencyHandler);

    shardingConfBuilder.addShard(shardConfig);
  }

  private void dropUserData(BackendBundle backendBundle) throws UserException {
    BackendService backendService = backendBundle.getExternalInterface().getBackendService();
    try (BackendConnection conn = backendService.openConnection();
        ExclusiveWriteBackendTransaction trans = conn.openExclusiveWriteTransaction()) {
      trans.dropUserData();
      trans.commit();
    }
  }

  private void startBundle(Bundle<?> bundle) {
    bundle.startAsync();
    bundle.awaitRunning();

    shutdowner.addStopShutdownListener(bundle);
  }

  private void resolveInconsistencies(BackendBundle backendBundle, 
      Map<String, ConsistencyHandler> consistencyHandlers) throws UserException,
      RetrierGiveUpException {

    Optional<String> inconsistentShard = consistencyHandlers.entrySet().stream()
        .filter(e -> !e.getValue().isConsistent())
        .map(Map.Entry::getKey)
        .findAny();

    //TODO: Some improvements can be done so only the inconsistent shards are dropped
    if (inconsistentShard.isPresent()) {
      LOGGER.warn("Found that replication shard {} is not consistent.",
          inconsistentShard.orElse("unknown")
      );
      LOGGER.warn("Dropping user data.");
      dropUserData(backendBundle);

      for (ConsistencyHandler consistencyHandler : consistencyHandlers.values()) {
        consistencyHandler.setConsistent(false);
      }
    } else {
      LOGGER.info("All replication shards are consistent");
    }
  }
}
