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

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.annotations.MongoWp;
import com.eightkdata.mongowp.server.MongoServerConfig;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.*;
import com.torodb.backend.derby.guice.DerbyBackendModule;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.guice.BackendModule;
import com.torodb.concurrent.DefaultConcurrentToolsFactory;
import com.torodb.concurrent.DefaultConcurrentToolsFactory.BlockerThreadFactoryFunction;
import com.torodb.concurrent.DefaultConcurrentToolsFactory.ForkJoinThreadFactoryFunction;
import com.torodb.concurrent.guice.ConcurrentModule;
import com.torodb.core.BuildProperties;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.annotations.TorodbRunnableService;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.guice.CoreModule;
import com.torodb.core.metrics.MetricsConfig;
import com.torodb.core.metrics.guice.MetricsModule;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.d2r.guice.D2RModule;
import com.torodb.metainfo.guice.MetainfModule;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.MongodServerConfig;
import com.torodb.mongodb.guice.MongoLayerModule;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.commands.ReplCommandsGuiceModule;
import com.torodb.mongodb.repl.guice.AkkaDbClonerProvider;
import com.torodb.mongodb.repl.guice.DocsPerTransaction;
import com.torodb.mongodb.repl.guice.MongoDbRepl;
import com.torodb.mongodb.repl.guice.MongoDbReplModule.DefaultCommitHeuristic;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplier.BatchLimits;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.cloner.CommitHeuristic;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodBundleFactory;
import com.torodb.torod.TorodServer;
import com.torodb.torod.guice.MemoryTorodModule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class OplogTestContextResourceRule extends ExternalResource {

  private static final Logger LOGGER =
      LogManager.getLogger(OplogTestContextResourceRule.class);
  private final Supplier<Module> specificModuleSupplier;

  private Injector testInjector;
  private MongodServer mongodServer;
  private OplogApplier oplogApplier;
  private TorodServer torodServer;
  private OplogManager oplogManager;
  private OplogTestContext testContext;
  private AnalyzedOplogBatchExecutor aobe;

  public OplogTestContextResourceRule(Supplier<Module> specificModuleSupplier) {
    this.specificModuleSupplier = specificModuleSupplier;
  }

  public OplogTestContext getTestContext() {
    return testContext;
  }

  @Override
  protected void before() throws Throwable {

    testInjector = Guice.createInjector(
        new ReplTestModule(),
        new TorodServerTestModule(),
        new CoreModule(),
        new BackendModule(),
        new DerbyBackendModule(),
        new MetainfModule(),
        new D2RModule(),
        new MemoryTorodModule(),
        new MetricsModule(new MetricsConfig() {
          @Override
          public Boolean getMetricsEnabled() {
            return true;
          }
        }),
        new ConcurrentModule(),
        new MongoLayerModule(),
        new MongodServerTestModule(),
        specificModuleSupplier.get()
    );

    torodServer = testInjector.getInstance(TorodBundle.class)
        .getTorodServer();
    torodServer.startAsync();

    mongodServer = testInjector.getInstance(MongodServer.class);
    mongodServer.startAsync();
    mongodServer.awaitRunning();

    assert mongodServer.getTorodServer().equals(torodServer);

    torodServer.awaitRunning();

    oplogManager = testInjector.getInstance(OplogManager.class);
    oplogManager.startAsync();
    oplogManager.awaitRunning();

    aobe = testInjector.getInstance(
        AnalyzedOplogBatchExecutor.class);
    aobe.startAsync();
    aobe.awaitRunning();

    oplogApplier = testInjector.getInstance(OplogApplier.class);

    testContext = new DefaultOplogTestContext(
        mongodServer,
        oplogApplier
    );
  }

  @Override
  protected void after() {
    if (oplogApplier != null) {
      try {
        oplogApplier.close();
      } catch (Exception ex) {
      }
    }

    if (aobe != null) {
      aobe.stopAsync();
      aobe.awaitTerminated();
    }

    if (oplogManager != null) {
      oplogManager.stopAsync();
      oplogManager.awaitTerminated();
    }

    if (mongodServer != null) {
      mongodServer.stopAsync();
      mongodServer.awaitTerminated();
    }

    if (torodServer != null) {
      torodServer.stopAsync();
      torodServer.awaitTerminated();
    }
  }

  /**
   * A Guice module that simmulates the modules added on packaging project.
   */
  private static class ReplTestModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(Clock.class)
          .toInstance(Clock.systemUTC());

      bind(DerbyDbBackendConfiguration.class)
          .toInstance(new TestDerbyDbBackendConfiguration());
      bind(BuildProperties.class)
          .toInstance(new TestBuildProperties());

      bind(Integer.class)
          .annotatedWith(ParallelLevel.class)
          .toInstance(Runtime.getRuntime().availableProcessors());

      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setNameFormat("torodb-executor-%d")
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

      bind(MongodServerConfig.class)
          .toInstance(new MongodServerConfig(HostAndPort.fromParts("localhost", 28017)));
      bind(MongoServerConfig.class)
          .to(MongodServerConfig.class);

      bind(Supervisor.class)
          .annotatedWith(MongoDbRepl.class)
          .to(TestReplSupervisor.class);

      install(new ReplCommandsGuiceModule());
    }

    private static class TestReplSupervisor implements Supervisor {

      @Override
      public SupervisorDecision onError(Object supervised, Throwable error) {
        LOGGER.error("Error on " + supervised, error);
        return SupervisorDecision.STOP;
      }

    }
  }

  private static class TestDerbyDbBackendConfiguration implements DerbyDbBackendConfiguration {

    @Override
    public boolean inMemory() {
      return true;
    }

    @Override
    public boolean embedded() {
      return true;
    }

    @Override
    public long getCursorTimeout() {
      return 10L * 60 * 1000;
    }

    @Override
    public long getConnectionPoolTimeout() {
      return 10_000;
    }

    @Override
    public int getConnectionPoolSize() {
      return 30;
    }

    @Override
    public int getReservedReadPoolSize() {
      return 10;
    }

    @Override
    public String getUsername() {
      return "torodb";
    }

    @Override
    public String getPassword() {
      return null;
    }

    @Override
    public String getDbHost() {
      return "localhost";
    }

    @Override
    public String getDbName() {
      return "torod";
    }

    @Override
    public int getDbPort() {
      return 1527;
    }

    @Override
    public boolean includeForeignKeys() {
      return false;
    }
  }

  private static class MongodServerTestModule extends PrivateModule {

    @Override
    protected void configure() {
      bind(OplogApplier.class)
          .to(DefaultOplogApplier.class)
          .in(Singleton.class);
      expose(OplogApplier.class);

      bind(DefaultOplogApplier.BatchLimits.class)
          .toInstance(new BatchLimits(1000, Duration.ofSeconds(2)));
    }

    @Provides
    TorodServer getMongodServer(TorodBundle bundle) {
      return bundle.getTorodServer();
    }
  }

  private static class TorodServerTestModule extends AbstractModule {

    @Override
    protected void configure() {

      bind(DbCloner.class)
          .annotatedWith(MongoDbRepl.class)
          .toProvider(AkkaDbClonerProvider.class);

      bind(CommitHeuristic.class)
          .to(DefaultCommitHeuristic.class)
          .in(Singleton.class);

      bind(Integer.class)
          .annotatedWith(DocsPerTransaction.class)
          .toInstance(1000);

      bind(ThreadFactory.class)
          .annotatedWith(MongoDbRepl.class)
          .toInstance(new ThreadFactoryBuilder()
              .setNameFormat("repl-unnamed-%d")
              .build()
          );

      bind(DefaultConcurrentToolsFactory.BlockerThreadFactoryFunction.class)
          .toInstance(new BlockerThreadFactoryFunction() {
            @Override
            public ThreadFactory apply(String prefix) {
              return new ThreadFactoryBuilder()
                  .setNameFormat(prefix + " -%d")
                  .build();
            }
          });

      bind(DefaultConcurrentToolsFactory.ForkJoinThreadFactoryFunction.class)
          .toInstance(new ForkJoinThreadFactoryFunction() {
            @Override
            public ForkJoinWorkerThreadFactory apply(String prefix) {
              return new ForkJoinWorkerThreadFactory() {
                private volatile int idProvider = 0;

                @Override
                public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                  ForkJoinWorkerThread newThread =
                      ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                  int id = idProvider++;
                  newThread.setName(prefix + '-' + id);
                  return newThread;
                }
              };
            }
          });
    }

    @Provides
    @Singleton
    BackendBundle createBackendBundle(BackendBundleFactory factory) {
      return factory.createBundle((o, t) -> SupervisorDecision.STOP);
    }

    @Provides
    @Singleton
    TorodBundle createTorodBundle(TorodBundleFactory factory, BackendBundle backendBundle) {
      return factory.createBundle((o, t) -> SupervisorDecision.STOP, backendBundle);
    }

  }

  private static class TestBuildProperties implements BuildProperties {

    public static final String BUILD_PROPERTIES_FILE = "ToroDB.build.properties";
    public static final Pattern FULL_VERSION_PATTERN =
        Pattern.compile("(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:-(.+))?");

    private final String fullVersion;
    private final int majorVersion;
    private final int minorVersion;
    private final int subVersion;
    private final String extraVersion;
    private final Instant buildTime;
    private final String gitCommitId;
    private final String gitBranch;
    private final String gitRemoteOriginURL;
    private final String javaVersion;
    private final String javaVendor;
    private final String javaVMSpecificationVersion;
    private final String javaVMVersion;
    private final String osName;
    private final String osArch;
    private final String osVersion;

    public TestBuildProperties() {

      fullVersion = "3.2.0";
      Matcher matcher = FULL_VERSION_PATTERN.matcher(fullVersion);
      if (!matcher.matches()) {
        throw new RuntimeException("Invalid version string '" + fullVersion + "'");
      }
      majorVersion = Integer.parseInt(matcher.group(1));
      minorVersion = Integer.parseInt(matcher.group(2));
      subVersion = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
      extraVersion = matcher.group(4);

      // DateUtils.parseDate may be replaced by SimpleDateFormat if using Java7
      try {
        buildTime = Instant.now();
      } catch (DateTimeParseException e) {
        throw new RuntimeException("buildTimestamp property not in ISO8601 format", e);
      }

      gitCommitId = "aCommitId";
      gitBranch = "aGitBranch";
      gitRemoteOriginURL = "aGitRemoteOriginURL";

      javaVersion = "aJavaVersion";
      javaVendor = "aJavaVendor";
      javaVMSpecificationVersion = "aJavaVMSpecificationVersion";
      javaVMVersion = "aJavaVMVersion";

      osName = "aOsName";
      osArch = "aOsArch";
      osVersion = "aOsVersion";
    }

    @Override
    public String getFullVersion() {
      return fullVersion;
    }

    @Override
    public int getMajorVersion() {
      return majorVersion;
    }

    @Override
    public int getMinorVersion() {
      return minorVersion;
    }

    @Override
    public int getSubVersion() {
      return subVersion;
    }

    @Override
    public String getExtraVersion() {
      return extraVersion;
    }

    @Override
    public Instant getBuildTime() {
      return buildTime;
    }

    @Override
    public String getGitCommitId() {
      return gitCommitId;
    }

    @Override
    public String getGitBranch() {
      return gitBranch;
    }

    public String getGitRemoteOriginUrl() {
      return gitRemoteOriginURL;
    }

    @Override
    public String getJavaVersion() {
      return javaVersion;
    }

    @Override
    public String getJavaVendor() {
      return javaVendor;
    }

    @Override
    public String getJavaVmSpecificationVersion() {
      return javaVMSpecificationVersion;
    }

    @Override
    public String getJavaVmVersion() {
      return javaVMVersion;
    }

    @Override
    public String getOsName() {
      return osName;
    }

    @Override
    public String getOsArch() {
      return osArch;
    }

    @Override
    public String getOsVersion() {
      return osVersion;
    }
  }

}
