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

package com.torodb.mongodb.commands.signatures.diagnostic;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonObjectId;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.ArrayField;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DateTimeField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.ObjectIdField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.ImmutableList;
import com.torodb.mongodb.commands.signatures.diagnostic.ServerStatusCommand.ServerStatusArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.ServerStatusCommand.ServerStatusReply;

import java.time.Instant;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
public class ServerStatusCommand
    extends AbstractNotAliasableCommand<ServerStatusArgument, ServerStatusReply> {

  public static final ServerStatusCommand INSTANCE = new ServerStatusCommand();

  private ServerStatusCommand() {
    super("serverStatus");
  }

  @Override
  public boolean isSlaveOk() {
    return true;
  }

  @Override
  public Class<? extends ServerStatusArgument> getArgClass() {
    return ServerStatusArgument.class;
  }

  @Override
  public ServerStatusArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, BadValueException, NoSuchKeyException {
    return ServerStatusArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(ServerStatusArgument request) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @Override
  public Class<? extends ServerStatusReply> getResultClass() {
    return ServerStatusReply.class;
  }

  @Override
  public BsonDocument marshallResult(ServerStatusReply reply) {
    return reply.marshall();
  }

  @Override
  public ServerStatusReply unmarshallResult(BsonDocument resultDoc) throws
      BadValueException, TypesMismatchException, NoSuchKeyException {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @Immutable
  public static class ServerStatusArgument {

    private static final BooleanField HOST_FIELD = new BooleanField("host");
    private static final BooleanField VERSION_FIELD = new BooleanField("version");
    private static final BooleanField PROCESS_FIELD = new BooleanField("process");
    private static final BooleanField PID_FIELD = new BooleanField("pid");
    private static final BooleanField UPTIME_FIELD = new BooleanField("uptime");
    private static final BooleanField UPTIME_ESTIMATE_FIELD = new BooleanField("uptimeEstimate");
    private static final BooleanField LOCAL_TIME_FIELD = new BooleanField("localTime");
    private static final BooleanField LOCKS_FIELD = new BooleanField("locks");
    private static final BooleanField GLOBAL_LOCK_FIELD = new BooleanField("globalLock");
    private static final BooleanField MEM_FIELD = new BooleanField("mem");
    private static final BooleanField CONNECTIONS_FIELD = new BooleanField("connections");
    private static final BooleanField EXTRA_INFO_FIELD = new BooleanField("extra_info");
    private static final BooleanField BACKGROUND_FLUSHING_FIELD = new BooleanField(
        "backgroundFlushing");
    private static final BooleanField CURSORS_FIELD = new BooleanField("cursors");
    private static final BooleanField NETWORK_FIELD = new BooleanField("network");
    private static final BooleanField REPL_FIELD = new BooleanField("repl");
    private static final BooleanField OPCOUNTERS_REPL_FIELD = new BooleanField("opcountersRepl");
    private static final BooleanField OPCOUNTERS_FIELD = new BooleanField("opcounters");
    private static final BooleanField RANGE_DELETER_FIELD = new BooleanField("rangeDeleter");
    private static final BooleanField SECURITY_FIELD = new BooleanField("security");
    private static final BooleanField STORAGE_ENGINE_FIELD = new BooleanField("storageEngine");
    private static final BooleanField ASSERTS_FIELD = new BooleanField("asserts");
    private static final BooleanField WRITE_BACKS_QUEUED_FIELD =
        new BooleanField("writeBacksQueued");
    private static final BooleanField DUR_FIELD = new BooleanField("dur");
    private static final BooleanField METRICS_FIELD = new BooleanField("metrics");
    private static final BooleanField WIRED_TIGER_FIELD = new BooleanField("wiredTiger");

    private final boolean host;
    private final boolean version;
    private final boolean process;
    private final boolean pid;
    private final boolean uptime;
    private final boolean uptimeEstimate;
    private final boolean localTime;
    private final boolean locks;
    private final boolean globalLock;
    private final boolean mem;
    private final boolean connections;
    private final boolean extraInfo;
    private final boolean backgroundFlushing;
    private final boolean cursors;
    private final boolean network;
    private final boolean repl;
    private final boolean opcountersRepl;
    private final boolean opcounters;
    private final boolean rangeDeleter;
    private final boolean security;
    private final boolean storageEngine;
    private final boolean asserts;
    private final boolean writeBacksQueued;
    private final boolean dur;
    private final boolean metrics;
    private final boolean wiredTiger;

    public ServerStatusArgument(boolean host, boolean version, boolean process, boolean pid,
        boolean uptime,
        boolean uptimeEstimate, boolean localTime, boolean locks, boolean globalLock, boolean mem,
        boolean connections, boolean extraInfo, boolean backgroundFlushing, boolean cursors,
        boolean network,
        boolean repl, boolean opcountersRepl, boolean opcounters, boolean rangeDeleter,
        boolean security,
        boolean storageEngine, boolean asserts, boolean writeBacksQueued, boolean dur,
        boolean metrics,
        boolean wiredTiger) {
      super();
      this.host = host;
      this.version = version;
      this.process = process;
      this.pid = pid;
      this.uptime = uptime;
      this.uptimeEstimate = uptimeEstimate;
      this.localTime = localTime;
      this.locks = locks;
      this.globalLock = globalLock;
      this.mem = mem;
      this.connections = connections;
      this.extraInfo = extraInfo;
      this.backgroundFlushing = backgroundFlushing;
      this.cursors = cursors;
      this.network = network;
      this.repl = repl;
      this.opcountersRepl = opcountersRepl;
      this.opcounters = opcounters;
      this.rangeDeleter = rangeDeleter;
      this.security = security;
      this.storageEngine = storageEngine;
      this.asserts = asserts;
      this.writeBacksQueued = writeBacksQueued;
      this.dur = dur;
      this.metrics = metrics;
      this.wiredTiger = wiredTiger;
    }

    public boolean isHost() {
      return host;
    }

    public boolean isVersion() {
      return version;
    }

    public boolean isProcess() {
      return process;
    }

    public boolean isPid() {
      return pid;
    }

    public boolean isUptime() {
      return uptime;
    }

    public boolean isUptimeEstimate() {
      return uptimeEstimate;
    }

    public boolean isLocalTime() {
      return localTime;
    }

    public boolean isLocks() {
      return locks;
    }

    public boolean isGlobalLock() {
      return globalLock;
    }

    public boolean isMem() {
      return mem;
    }

    public boolean isConnections() {
      return connections;
    }

    public boolean isExtraInfo() {
      return extraInfo;
    }

    public boolean isBackgroundFlushing() {
      return backgroundFlushing;
    }

    public boolean isCursors() {
      return cursors;
    }

    public boolean isNetwork() {
      return network;
    }

    public boolean isRepl() {
      return repl;
    }

    public boolean isOpcountersRepl() {
      return opcountersRepl;
    }

    public boolean isOpcounters() {
      return opcounters;
    }

    public boolean isRangeDeleter() {
      return rangeDeleter;
    }

    public boolean isSecurity() {
      return security;
    }

    public boolean isStorageEngine() {
      return storageEngine;
    }

    public boolean isAsserts() {
      return asserts;
    }

    public boolean isDur() {
      return dur;
    }

    public boolean isWriteBacksQueued() {
      return writeBacksQueued;
    }

    public boolean isMetrics() {
      return metrics;
    }

    public boolean isWiredTiger() {
      return wiredTiger;
    }

    protected static ServerStatusArgument unmarshall(BsonDocument doc)
        throws TypesMismatchException, BadValueException, NoSuchKeyException {
      boolean host = BsonReaderTool.getBooleanOrNumeric(doc, HOST_FIELD, true);
      boolean version = BsonReaderTool.getBooleanOrNumeric(doc, VERSION_FIELD, true);
      boolean process = BsonReaderTool.getBooleanOrNumeric(doc, PROCESS_FIELD, true);
      boolean pid = BsonReaderTool.getBooleanOrNumeric(doc, PID_FIELD, true);
      boolean uptime = BsonReaderTool.getBooleanOrNumeric(doc, UPTIME_FIELD, true);
      boolean uptimeEstimate = BsonReaderTool.getBooleanOrNumeric(doc, UPTIME_ESTIMATE_FIELD, true);
      boolean localTime = BsonReaderTool.getBooleanOrNumeric(doc, LOCAL_TIME_FIELD, true);
      boolean locks = BsonReaderTool.getBooleanOrNumeric(doc, LOCKS_FIELD, true);
      boolean globalLock = BsonReaderTool.getBooleanOrNumeric(doc, GLOBAL_LOCK_FIELD, true);
      boolean mem = BsonReaderTool.getBooleanOrNumeric(doc, MEM_FIELD, true);
      boolean connections = BsonReaderTool.getBooleanOrNumeric(doc, CONNECTIONS_FIELD, true);
      boolean extraInfo = BsonReaderTool.getBooleanOrNumeric(doc, EXTRA_INFO_FIELD, true);
      boolean backgroundFlushing = BsonReaderTool
          .getBooleanOrNumeric(doc, BACKGROUND_FLUSHING_FIELD, true);
      boolean cursors = BsonReaderTool.getBooleanOrNumeric(doc, CURSORS_FIELD, true);
      boolean network = BsonReaderTool.getBooleanOrNumeric(doc, NETWORK_FIELD, true);
      boolean repl = BsonReaderTool.getBooleanOrNumeric(doc, REPL_FIELD, true);
      boolean opcountersRepl = BsonReaderTool.getBooleanOrNumeric(doc, OPCOUNTERS_REPL_FIELD, true);
      boolean opcounters = BsonReaderTool.getBooleanOrNumeric(doc, OPCOUNTERS_FIELD, true);
      boolean rangeDeleter = BsonReaderTool.getBooleanOrNumeric(doc, RANGE_DELETER_FIELD, false);
      boolean security = BsonReaderTool.getBooleanOrNumeric(doc, SECURITY_FIELD, true);
      boolean storageEngine = BsonReaderTool.getBooleanOrNumeric(doc, STORAGE_ENGINE_FIELD, true);
      boolean asserts = BsonReaderTool.getBooleanOrNumeric(doc, ASSERTS_FIELD, true);
      boolean writeBacksQueued = BsonReaderTool.getBooleanOrNumeric(doc, WRITE_BACKS_QUEUED_FIELD,
          true);
      boolean dur = BsonReaderTool.getBooleanOrNumeric(doc, DUR_FIELD, true);
      boolean metrics = BsonReaderTool.getBooleanOrNumeric(doc, METRICS_FIELD, true);
      boolean wiredTiger = BsonReaderTool.getBooleanOrNumeric(doc, WIRED_TIGER_FIELD, true);

      return new ServerStatusArgument(host, version, process, pid, uptime, uptimeEstimate,
          localTime, locks, globalLock, mem, connections, extraInfo, backgroundFlushing, cursors,
          network, repl, opcountersRepl, opcounters, rangeDeleter, security, storageEngine,
          asserts, writeBacksQueued, dur, metrics, wiredTiger);
    }

  }

  //TODO(gortiz): This reply is not prepared to respond on error cases!
  public static class ServerStatusReply {

    private static final StringField HOST_FIELD = new StringField("host");
    private static final StringField VERSION_FIELD = new StringField("version");
    private static final StringField PROCESS_FIELD = new StringField("process");
    private static final IntField PID_FIELD = new IntField("pid");
    private static final LongField UPTIME_FIELD = new LongField("uptime");
    private static final LongField UPTIME_ESTIMATE_FIELD = new LongField("uptimeEstimate");
    private static final DateTimeField LOCAL_TIME_FIELD = new DateTimeField("localTime");
    private static final DocField LOCKS_FIELD = new DocField("locks");
    private static final DocField GLOBAL_LOCK_FIELD = new DocField("globalLock");
    private static final DocField MEM_FIELD = new DocField("mem");
    private static final DocField CONNECTIONS_FIELD = new DocField("connections");
    private static final DocField EXTRA_INFO_FIELD = new DocField("extra_info");
    private static final DocField BACKGROUND_FLUSHING_FIELD = new DocField("backgroundFlushing");
    private static final DocField CURSORS_FIELD = new DocField("cursors");
    private static final DocField NETWORK_FIELD = new DocField("network");
    private static final DocField REPL_FIELD = new DocField("repl");
    private static final DocField OPCOUNTERS_REPL_FIELD = new DocField("opcountersRepl");
    private static final DocField OPCOUNTERS_FIELD = new DocField("opcounters");
    private static final DocField RANGE_DELETER_FIELD = new DocField("rangeDeleter");
    private static final DocField SECURITY_FIELD = new DocField("security");
    private static final DocField STORAGE_ENGINE_FIELD = new DocField("storageEngine");
    private static final DocField ASSERTS_FIELD = new DocField("asserts");
    private static final IntField WRITE_BACKS_QUEUED_FIELD = new IntField("writeBacksQueued");
    private static final DocField DUR_FIELD = new DocField("dur");
    private static final DocField METRICS_FIELD = new DocField("metrics");
    private static final DocField WIRED_TIGER_FIELD = new DocField("wiredTiger");

    private final String host;
    private final String version;
    private final String process;
    private final Integer pid;
    private final Long uptime;
    private final Long uptimeEstimate;
    private final Instant localTime;
    private final Locks locks;
    private final GlobalLock globalLock;
    private final Mem mem;
    private final Connections connections;
    private final ExtraInfo extraInfo;
    private final BackgroundFlushing backgroundFlushing;
    private final Cursors cursors;
    private final Network network;
    private final Repl repl;
    private final Opcounters opcountersRepl;
    private final Opcounters opcounters;
    private final RangeDeleter rangeDeleter;
    private final Security security;
    private final StorageEngine storageEngine;
    private final Asserts asserts;
    private final Dur dur;
    private final Integer writebacksQueued;
    private final Metrics metrics;
    private final WiredTiger wiredTiger;

    public ServerStatusReply(String host, String version, String process, Integer pid, Long uptime,
        Long uptimeEstimate, Instant localTime, Locks locks, GlobalLock globalLock, Mem mem,
        Connections connections, ExtraInfo extraInfo, BackgroundFlushing backgroundFlushing,
        Cursors cursors,
        Network network, Repl repl, Opcounters opcountersRepl, Opcounters opcounters,
        RangeDeleter rangeDeleter,
        Security security, StorageEngine storageEngine, Asserts asserts, Dur dur,
        Integer writebacksQueued,
        Metrics metrics, WiredTiger wiredTiger) {
      super();
      this.host = host;
      this.version = version;
      this.process = process;
      this.pid = pid;
      this.uptime = uptime;
      this.uptimeEstimate = uptimeEstimate;
      this.localTime = localTime;
      this.locks = locks;
      this.globalLock = globalLock;
      this.mem = mem;
      this.connections = connections;
      this.extraInfo = extraInfo;
      this.backgroundFlushing = backgroundFlushing;
      this.cursors = cursors;
      this.network = network;
      this.repl = repl;
      this.opcountersRepl = opcountersRepl;
      this.opcounters = opcounters;
      this.rangeDeleter = rangeDeleter;
      this.security = security;
      this.storageEngine = storageEngine;
      this.asserts = asserts;
      this.dur = dur;
      this.writebacksQueued = writebacksQueued;
      this.metrics = metrics;
      this.wiredTiger = wiredTiger;
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();
      if (host != null) {
        builder.append(HOST_FIELD, host);
      }
      if (version != null) {
        builder.append(VERSION_FIELD, version);
      }
      if (process != null) {
        builder.append(PROCESS_FIELD, process);
      }
      if (pid != null) {
        builder.append(PID_FIELD, pid);
      }
      if (uptime != null) {
        builder.append(UPTIME_FIELD, uptime);
      }
      if (uptimeEstimate != null) {
        builder.append(UPTIME_ESTIMATE_FIELD, uptimeEstimate);
      }
      if (localTime != null) {
        builder.append(LOCAL_TIME_FIELD, localTime);
      }
      if (locks != null) {
        builder.append(LOCKS_FIELD, locks.marshall());
      }
      if (globalLock != null) {
        builder.append(GLOBAL_LOCK_FIELD, globalLock.marshall());
      }
      if (mem != null) {
        builder.append(MEM_FIELD, mem.marshall());
      }
      if (connections != null) {
        builder.append(CONNECTIONS_FIELD, connections.marshall());
      }
      if (extraInfo != null) {
        builder.append(EXTRA_INFO_FIELD, extraInfo.marshall());
      }
      if (backgroundFlushing != null) {
        builder.append(BACKGROUND_FLUSHING_FIELD, backgroundFlushing.marshall());
      }
      if (cursors != null) {
        builder.append(CURSORS_FIELD, cursors.marshall());
      }
      if (network != null) {
        builder.append(NETWORK_FIELD, network.marshall());
      }
      if (repl != null) {
        builder.append(REPL_FIELD, repl.marshall());
      }
      if (opcountersRepl != null) {
        builder.append(OPCOUNTERS_REPL_FIELD, opcountersRepl.marshall());
      }
      if (opcounters != null) {
        builder.append(OPCOUNTERS_FIELD, opcounters.marshall());
      }
      if (rangeDeleter != null) {
        builder.append(RANGE_DELETER_FIELD, rangeDeleter.marshall());
      }
      if (security != null) {
        builder.append(SECURITY_FIELD, security.marshall());
      }
      if (storageEngine != null) {
        builder.append(STORAGE_ENGINE_FIELD, storageEngine.marshall());
      }
      if (asserts != null) {
        builder.append(ASSERTS_FIELD, asserts.marshall());
      }
      if (dur != null) {
        builder.append(DUR_FIELD, dur.marshall());
      }
      if (writebacksQueued != null) {
        builder.append(WRITE_BACKS_QUEUED_FIELD, writebacksQueued);
      }
      if (metrics != null) {
        builder.append(METRICS_FIELD, metrics.marshall());
      }
      if (wiredTiger != null) {
        builder.append(WIRED_TIGER_FIELD, wiredTiger.marshall());
      }
      return builder.build();
    }

    public String getHost() {
      return host;
    }

    public String getVersion() {
      return version;
    }

    public String getProcess() {
      return process;
    }

    public Integer getPid() {
      return pid;
    }

    public Long getUptime() {
      return uptime;
    }

    public Long getUptimeEstimate() {
      return uptimeEstimate;
    }

    public Instant getLocalTime() {
      return localTime;
    }

    public Locks getLocks() {
      return locks;
    }

    public GlobalLock getGlobalLock() {
      return globalLock;
    }

    public Mem getMem() {
      return mem;
    }

    public Connections getConnections() {
      return connections;
    }

    public ExtraInfo getExtraInfo() {
      return extraInfo;
    }

    public BackgroundFlushing getBackgroundFlushing() {
      return backgroundFlushing;
    }

    public Cursors getCursors() {
      return cursors;
    }

    public Network getNetwork() {
      return network;
    }

    public Repl getRepl() {
      return repl;
    }

    public Opcounters getOpcountersRepl() {
      return opcountersRepl;
    }

    public Opcounters getOpcounters() {
      return opcounters;
    }

    public RangeDeleter getRangeDeleter() {
      return rangeDeleter;
    }

    public Security getSecurity() {
      return security;
    }

    public StorageEngine getStorageEngine() {
      return storageEngine;
    }

    public Asserts getAsserts() {
      return asserts;
    }

    public Dur getDur() {
      return dur;
    }

    public Integer getWritebacksQueued() {
      return writebacksQueued;
    }

    public Metrics getMetrics() {
      return metrics;
    }

    public WiredTiger getWiredTiger() {
      return wiredTiger;
    }

    public static class Builder {

      private String host;
      private String version;
      private String process;
      private Integer pid;
      private Long uptime;
      private Long uptimeEstimate;
      private Instant localTime;
      private Locks locks;
      private GlobalLock globalLock;
      private Mem mem;
      private Connections connections;
      private ExtraInfo extraInfo;
      private BackgroundFlushing backgroundFlushing;
      private Cursors cursors;
      private Network network;
      private Repl repl;
      private Opcounters opcountersRepl;
      private Opcounters opcounters;
      private RangeDeleter rangeDeleter;
      private Security security;
      private StorageEngine storageEngine;
      private Asserts asserts;
      private Dur dur;
      private Integer writebacksQueued;
      private Metrics metrics;
      private WiredTiger wiredTiger;

      public Builder() {
      }

      public String getHost() {
        return host;
      }

      public Builder setHost(String host) {
        this.host = host;
        return this;
      }

      public String getVersion() {
        return version;
      }

      public Builder setVersion(String version) {
        this.version = version;
        return this;
      }

      public String getProcess() {
        return process;
      }

      public Builder setProcess(String process) {
        this.process = process;
        return this;
      }

      public Integer getPid() {
        return pid;
      }

      public Builder setPid(Integer pid) {
        this.pid = pid;
        return this;
      }

      public Long getUptime() {
        return uptime;
      }

      public Builder setUptime(Long uptime) {
        this.uptime = uptime;
        return this;
      }

      public Long getUptimeEstimate() {
        return uptimeEstimate;
      }

      public Builder setUptimeEstimate(Long uptimeEstimate) {
        this.uptimeEstimate = uptimeEstimate;
        return this;
      }

      public Instant getLocalTime() {
        return localTime;
      }

      public Builder setLocalTime(Instant localTime) {
        this.localTime = localTime;
        return this;
      }

      public Locks getLocks() {
        return locks;
      }

      public Builder setLocks(Locks locks) {
        this.locks = locks;
        return this;
      }

      public GlobalLock getGlobalLock() {
        return globalLock;
      }

      public Builder setGlobalLock(GlobalLock globalLock) {
        this.globalLock = globalLock;
        return this;
      }

      public Mem getMem() {
        return mem;
      }

      public Builder setMem(Mem mem) {
        this.mem = mem;
        return this;
      }

      public Connections getConnections() {
        return connections;
      }

      public Builder setConnections(Connections connections) {
        this.connections = connections;
        return this;
      }

      public ExtraInfo getExtraInfo() {
        return extraInfo;
      }

      public Builder setExtraInfo(ExtraInfo extraInfo) {
        this.extraInfo = extraInfo;
        return this;
      }

      public BackgroundFlushing getBackgroundFlushing() {
        return backgroundFlushing;
      }

      public Builder setBackgroundFlushing(BackgroundFlushing backgroundFlushing) {
        this.backgroundFlushing = backgroundFlushing;
        return this;
      }

      public Cursors getCursors() {
        return cursors;
      }

      public Builder setCursors(Cursors cursors) {
        this.cursors = cursors;
        return this;
      }

      public Network getNetwork() {
        return network;
      }

      public Builder setNetwork(Network network) {
        this.network = network;
        return this;
      }

      public Repl getRepl() {
        return repl;
      }

      public Builder setRepl(Repl repl) {
        this.repl = repl;
        return this;
      }

      public Opcounters getOpcountersRepl() {
        return opcountersRepl;
      }

      public Builder setOpcountersRepl(Opcounters opcountersRepl) {
        this.opcountersRepl = opcountersRepl;
        return this;
      }

      public Opcounters getOpcounters() {
        return opcounters;
      }

      public Builder setOpcounters(Opcounters opcounters) {
        this.opcounters = opcounters;
        return this;
      }

      public RangeDeleter getRangeDeleter() {
        return rangeDeleter;
      }

      public Builder setRangeDeleter(RangeDeleter rangeDeleter) {
        this.rangeDeleter = rangeDeleter;
        return this;
      }

      public Security getSecurity() {
        return security;
      }

      public Builder setSecurity(Security security) {
        this.security = security;
        return this;
      }

      public StorageEngine getStorageEngine() {
        return storageEngine;
      }

      public Builder setStorageEngine(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
        return this;
      }

      public Asserts getAsserts() {
        return asserts;
      }

      public Builder setAsserts(Asserts asserts) {
        this.asserts = asserts;
        return this;
      }

      public Dur getDur() {
        return dur;
      }

      public Builder setDur(Dur dur) {
        this.dur = dur;
        return this;
      }

      public Integer getWritebacksQueued() {
        return writebacksQueued;
      }

      public Builder setWritebacksQueued(Integer writebacksQueued) {
        this.writebacksQueued = writebacksQueued;
        return this;
      }

      public Metrics getMetrics() {
        return metrics;
      }

      public Builder setMetrics(Metrics metrics) {
        this.metrics = metrics;
        return this;
      }

      public WiredTiger getWiredTiger() {
        return wiredTiger;
      }

      public Builder setWiredTiger(WiredTiger wiredTiger) {
        this.wiredTiger = wiredTiger;
        return this;
      }

      public ServerStatusReply build() {
        return new ServerStatusReply(
            host,
            version,
            process,
            pid,
            uptime,
            uptimeEstimate,
            localTime,
            locks,
            globalLock,
            mem,
            connections,
            extraInfo,
            backgroundFlushing,
            cursors,
            network,
            repl,
            opcountersRepl,
            opcounters,
            rangeDeleter,
            security,
            storageEngine,
            asserts,
            dur,
            writebacksQueued,
            metrics,
            wiredTiger);
      }
    }

  }

  public static class Locks {

    private static final DocField LOCKS_GLOBAL_FIELD = new DocField("Global");
    private static final DocField LOCKS_MMAPV1_JOURNAL_FIELD = new DocField("MMAPV1Journal");
    private static final DocField LOCKS_DATABASE_FIELD = new DocField("Database");
    private static final DocField LOCKS_COLLECTION_FIELD = new DocField("Collection");
    private static final DocField LOCKS_METADATA_FIELD = new DocField("Metadata");
    private static final DocField LOCKS_OPLOG_FIELD = new DocField("oplog");

    private final Lock global;
    private final Lock mmapv1Journal;
    private final Lock database;
    private final Lock collection;
    private final Lock metadata;
    private final Lock oplog;

    public Locks(Lock global, Lock mmapv1Journal, Lock database, Lock collection, Lock metadata,
        Lock oplog) {
      super();
      this.global = global;
      this.mmapv1Journal = mmapv1Journal;
      this.database = database;
      this.collection = collection;
      this.metadata = metadata;
      this.oplog = oplog;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(LOCKS_GLOBAL_FIELD, global.marshall())
          .append(LOCKS_MMAPV1_JOURNAL_FIELD, mmapv1Journal.marshall())
          .append(LOCKS_DATABASE_FIELD, database.marshall())
          .append(LOCKS_COLLECTION_FIELD, collection.marshall())
          .append(LOCKS_METADATA_FIELD, metadata.marshall())
          .append(LOCKS_OPLOG_FIELD, oplog.marshall())
          .build();
    }

    public Lock getGlobal() {
      return global;
    }

    public Lock getMmapv1journal() {
      return mmapv1Journal;
    }

    public Lock getDatabase() {
      return database;
    }

    public Lock getCollection() {
      return collection;
    }

    public Lock getMetadata() {
      return metadata;
    }

    public Lock getOplog() {
      return oplog;
    }

    public static class Lock {

      private static final DocField LOCKS_LOCK_AQUIRE_COUNT_FIELD = new DocField("acquireCount");
      private static final DocField LOCKS_LOCK_AQUIRE_WAIT_COUNT_FIELD = new DocField(
          "acquireWaitCount");
      private static final DocField LOCKS_LOCK_TIME_ACQUIRING_MICROS_COUNT_FIELD = new DocField(
          "timeAcquiringMicros");
      private static final DocField LOCKS_LOCK_DEADLOCK_COUNT_FIELD = new DocField("deadlockCount");

      private final Count acquireCount;
      private final Count acquireWaitCount;
      private final Count timeAcquiringMicros;
      private final Count deadlockCount;

      public Lock(Count acquireCount, Count acquireWaitCount, Count timeAcquiringMicros,
          Count deadlockCount) {
        super();
        this.acquireCount = acquireCount;
        this.acquireWaitCount = acquireWaitCount;
        this.timeAcquiringMicros = timeAcquiringMicros;
        this.deadlockCount = deadlockCount;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(LOCKS_LOCK_AQUIRE_COUNT_FIELD, acquireCount.marshall())
            .append(LOCKS_LOCK_AQUIRE_WAIT_COUNT_FIELD, acquireWaitCount.marshall())
            .append(LOCKS_LOCK_TIME_ACQUIRING_MICROS_COUNT_FIELD, timeAcquiringMicros.marshall())
            .append(LOCKS_LOCK_DEADLOCK_COUNT_FIELD, deadlockCount.marshall())
            .build();
      }

      public Count getAcquireCount() {
        return acquireCount;
      }

      public Count getAcquireWaitCount() {
        return acquireWaitCount;
      }

      public Count getTimeAcquiringMicros() {
        return timeAcquiringMicros;
      }

      public Count getDeadlockCount() {
        return deadlockCount;
      }
    }

    public static class Count {

      private static final IntField LOCKS_LOCK_R_LOWER_FIELD = new IntField("r");
      private static final IntField LOCKS_LOCK_W_LOWER_FIELD = new IntField("w");
      private static final IntField LOCKS_LOCK_R_UPPER_FIELD = new IntField("R");
      private static final IntField LOCKS_LOCK_W_UPPER_FIELD = new IntField("W");

      private final int intentShared;
      private final int intentExclusive;
      private final int shared;
      private final int exclusive;

      public Count(int intentShared, int intentExclusive, int shared, int exclusive) {
        super();
        this.intentShared = intentShared;
        this.intentExclusive = intentExclusive;
        this.shared = shared;
        this.exclusive = exclusive;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(LOCKS_LOCK_R_LOWER_FIELD, intentShared)
            .append(LOCKS_LOCK_W_LOWER_FIELD, intentExclusive)
            .append(LOCKS_LOCK_R_UPPER_FIELD, shared)
            .append(LOCKS_LOCK_W_UPPER_FIELD, exclusive)
            .build();
      }

      public int getIntentShared() {
        return intentShared;
      }

      public int getIntentExclusive() {
        return intentExclusive;
      }

      public int getExclusive() {
        return exclusive;
      }
    }
  }

  public static class GlobalLock {

    private static final LongField GLOBAL_LOCK_TOTAL_TIME_FIELD = new LongField("totalTime");
    private static final DocField GLOBAL_LOCK_CURRENT_QUEUE_FIELD = new DocField("currentQueue");
    private static final DocField GLOBAL_LOCK_ACTIVE_CLIENTS_FIELD = new DocField("activeClients");

    private final long totalTime;
    private final GlobalLockStats currentQueue;
    private final GlobalLockStats activeClients;

    public GlobalLock(long totalTime, GlobalLockStats currentQueue, GlobalLockStats activeClients) {
      super();
      this.totalTime = totalTime;
      this.currentQueue = currentQueue;
      this.activeClients = activeClients;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(GLOBAL_LOCK_TOTAL_TIME_FIELD, totalTime)
          .append(GLOBAL_LOCK_CURRENT_QUEUE_FIELD, currentQueue.marshall())
          .append(GLOBAL_LOCK_ACTIVE_CLIENTS_FIELD, activeClients.marshall())
          .build();
    }

    public long getTotalTime() {
      return totalTime;
    }

    public GlobalLockStats getCurrentQueue() {
      return currentQueue;
    }

    public GlobalLockStats getActiveClients() {
      return activeClients;
    }

    public static class GlobalLockStats {

      private static final IntField GLOBAL_LOCK_TOTAL_FIELD = new IntField("total");
      private static final IntField GLOBAL_LOCK_READERS_FIELD = new IntField("readers");
      private static final IntField GLOBAL_LOCK_WRITERS_FIELD = new IntField("writers");

      private final int total;
      private final int readers;
      private final int writers;

      public GlobalLockStats(int total, int readers, int writers) {
        super();
        this.total = total;
        this.readers = readers;
        this.writers = writers;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(GLOBAL_LOCK_TOTAL_FIELD, total)
            .append(GLOBAL_LOCK_READERS_FIELD, readers)
            .append(GLOBAL_LOCK_WRITERS_FIELD, writers)
            .build();
      }

      public int getTotal() {
        return total;
      }

      public int getReaders() {
        return readers;
      }

      public int getWriters() {
        return writers;
      }
    }
  }

  public static class Mem {

    private static final IntField MEM_BITS_FIELD = new IntField("bits");
    private static final LongField MEM_RESIDENT_FIELD = new LongField("resident");
    private static final LongField MEM_VIRTUAL_FIELD = new LongField("virtual");
    private static final BooleanField MEM_SUPPORTED_FIELD = new BooleanField("supported");
    private static final LongField MEM_MAPPED_FIELD = new LongField("mapped");
    private static final LongField MEM_MAPPED_WITH_JOURNAL_FIELD =
        new LongField("mappedWithJournal");
    private static final StringField MEM_NOTE_FIELD = new StringField("note");

    private final int bits;
    private final long resident;
    private final long virtual;
    private final boolean supported;
    private final long mapped;
    private final long mappedWithJournal;
    private final String note;

    public Mem(int bits, long resident, long virtual, boolean supported, long mapped,
        long mappedWithJournal,
        String note) {
      super();
      this.bits = bits;
      this.resident = resident;
      this.virtual = virtual;
      this.supported = supported;
      this.mapped = mapped;
      this.mappedWithJournal = mappedWithJournal;
      this.note = note;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(MEM_BITS_FIELD, bits)
          .append(MEM_RESIDENT_FIELD, resident)
          .append(MEM_VIRTUAL_FIELD, virtual)
          .append(MEM_SUPPORTED_FIELD, supported)
          .append(MEM_MAPPED_FIELD, mapped)
          .append(MEM_MAPPED_WITH_JOURNAL_FIELD, mappedWithJournal)
          .append(MEM_NOTE_FIELD, note)
          .build();
    }

    public int getBits() {
      return bits;
    }

    public long getResident() {
      return resident;
    }

    public long getVirtual() {
      return virtual;
    }

    public boolean isSupported() {
      return supported;
    }

    public long getMapped() {
      return mapped;
    }

    public long getMappedWithJournal() {
      return mappedWithJournal;
    }

    public String getNote() {
      return note;
    }
  }

  public static class Connections {

    private static final IntField CONNECTIONS_CURRENT_FIELD = new IntField("current");
    private static final IntField CONNECTIONS_AVAILABLE_FIELD = new IntField("available");
    private static final IntField CONNECTIONS_TOTAL_CREATED_FIELD = new IntField("totalCreated");

    private final int current;
    private final int available;
    private final int totalCreated;

    public Connections(int current, int available, int totalCreated) {
      super();
      this.current = current;
      this.available = available;
      this.totalCreated = totalCreated;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(CONNECTIONS_CURRENT_FIELD, current)
          .append(CONNECTIONS_AVAILABLE_FIELD, available)
          .append(CONNECTIONS_TOTAL_CREATED_FIELD, totalCreated)
          .build();
    }

    public int getCurrent() {
      return current;
    }

    public int getAvailable() {
      return available;
    }

    public int getTotalCreated() {
      return totalCreated;
    }
  }

  public static class ExtraInfo {

    private static final StringField EXTRA_INFO_NOTE_FIELD = new StringField("note");
    private static final LongField EXTRA_INFO_HEAP_USAGE_BYTES_FIELD = new LongField(
        "heap_usage_bytes");
    private static final IntField EXTRA_INFO_FAULTS_FIELD = new IntField("page_faults");

    private final String note;
    private final long heapUsageBytes;
    private final int pageFaults;

    public ExtraInfo(String note, long heapUsageBytes, int pageFaults) {
      super();
      this.note = note;
      this.heapUsageBytes = heapUsageBytes;
      this.pageFaults = pageFaults;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(EXTRA_INFO_NOTE_FIELD, note)
          .append(EXTRA_INFO_HEAP_USAGE_BYTES_FIELD, heapUsageBytes)
          .append(EXTRA_INFO_FAULTS_FIELD, pageFaults)
          .build();
    }

    public String getNote() {
      return note;
    }

    public long getHeapUsageBytes() {
      return heapUsageBytes;
    }

    public int getPageFaults() {
      return pageFaults;
    }
  }

  public static class BackgroundFlushing {

    private static final IntField BACKGROUND_FLUSHING_FLUSHES_FIELD = new IntField("flushes");
    private static final LongField BACKGROUND_FLUSHING_TOTAL_MS_FIELD = new LongField("total_ms");
    private static final LongField BACKGROUND_FLUSHING_AVERAGE_MS_FIELD =
        new LongField("average_ms");
    private static final LongField BACKGROUND_FLUSHING_LAST_MS_FIELD = new LongField("last_ms");
    private static final DateTimeField BACKGROUND_FLUSHING_LAST_FINISHED_FIELD = new DateTimeField(
        "last_finished");

    private final int flushes;
    private final long totalMs;
    private final long averageMs;
    private final long lastMs;
    private final Instant lastFinished;

    public BackgroundFlushing(int flushes, long totalMs, long averageMs, long lastMs,
        @Nonnull Instant lastFinished) {
      super();
      this.flushes = flushes;
      this.totalMs = totalMs;
      this.averageMs = averageMs;
      this.lastMs = lastMs;
      this.lastFinished = lastFinished;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(BACKGROUND_FLUSHING_FLUSHES_FIELD, flushes)
          .append(BACKGROUND_FLUSHING_TOTAL_MS_FIELD, totalMs)
          .append(BACKGROUND_FLUSHING_AVERAGE_MS_FIELD, averageMs)
          .append(BACKGROUND_FLUSHING_LAST_MS_FIELD, lastMs)
          .append(BACKGROUND_FLUSHING_LAST_FINISHED_FIELD, lastFinished)
          .build();
    }

    public int getFlushes() {
      return flushes;
    }

    public long getTotalMs() {
      return totalMs;
    }

    public long getAverageMs() {
      return averageMs;
    }

    public long getLastMs() {
      return lastMs;
    }

    public Instant getLastFinished() {
      return lastFinished;
    }
  }

  public static class Cursors {

    private static final StringField CURSORS_NOTE_FIELD = new StringField("note");
    private static final IntField CURSORS_TOTAL_OPEN_FIELD = new IntField("totalOpen");
    private static final LongField CURSORS_CLIENT_CUSRORS_SIZE_FIELD = new LongField(
        "clientCursors_size");
    private static final IntField CURSORS_TIMED_OUT_FIELD = new IntField("timedOut");
    private static final IntField CURSORS_TOTAL_NO_TIMEOUT_FIELD = new IntField("totalNoTimeout");
    private static final IntField CURSORS_PINNED_FIELD = new IntField("pinned");

    private final String note;
    private final int totalOpen;
    private final long clientCursorsSize;
    private final int timedOut;
    private final int totalNoTimeout;
    private final int pinned;

    public Cursors(String note, int totalOpen, long clientCursorsSize, int timedOut,
        int totalNoTimeout,
        int pinned) {
      super();
      this.note = note;
      this.totalOpen = totalOpen;
      this.clientCursorsSize = clientCursorsSize;
      this.timedOut = timedOut;
      this.totalNoTimeout = totalNoTimeout;
      this.pinned = pinned;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(CURSORS_NOTE_FIELD, note)
          .append(CURSORS_TOTAL_OPEN_FIELD, totalOpen)
          .append(CURSORS_CLIENT_CUSRORS_SIZE_FIELD, clientCursorsSize)
          .append(CURSORS_TIMED_OUT_FIELD, timedOut)
          .append(CURSORS_TOTAL_NO_TIMEOUT_FIELD, totalNoTimeout)
          .append(CURSORS_PINNED_FIELD, pinned)
          .build();
    }

    public String getNote() {
      return note;
    }

    public int getTotalOpen() {
      return totalOpen;
    }

    public int getTimedOut() {
      return timedOut;
    }

    public int getTotalNoTimeout() {
      return totalNoTimeout;
    }

    public int getPinned() {
      return pinned;
    }
  }

  public static class Network {

    private static final LongField NETWORK_BYTES_IN_FIELD = new LongField("bytesIn");
    private static final LongField NETWORK_BYTES_OUT_FIELD = new LongField("bytesOut");
    private static final IntField NETWORK_NUM_REQUESTS_FIELD = new IntField("numRequests");

    private final long bytesIn;
    private final long bytesOut;
    private final int numRequests;

    public Network(long bytesIn, long bytesOut, int numRequests) {
      super();
      this.bytesIn = bytesIn;
      this.bytesOut = bytesOut;
      this.numRequests = numRequests;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(NETWORK_BYTES_IN_FIELD, bytesIn)
          .append(NETWORK_BYTES_OUT_FIELD, bytesOut)
          .append(NETWORK_NUM_REQUESTS_FIELD, numRequests)
          .build();
    }

    public long getBytesIn() {
      return bytesIn;
    }

    public long getBytesOut() {
      return bytesOut;
    }

    public int getNumRequests() {
      return numRequests;
    }
  }

  public static class Repl {

    private static final StringField REPL_SET_NAME_FIELD = new StringField("setName");
    private static final BooleanField REPL_ISMASTER_FIELD = new BooleanField("ismaster");
    private static final BooleanField REPL_SECONDARY_FIELD = new BooleanField("secondary");
    private static final StringField REPL_PRIMARY_FIELD = new StringField("primary");
    private static final ArrayField REPL_HOSTS_FIELD = new ArrayField("hosts");
    private static final StringField REPL_ME_FIELD = new StringField("me");
    private static final ObjectIdField REPL_ELECTION_ID_FIELD = new ObjectIdField("electionId");
    private static final LongField REPL_RBID_FIELD = new LongField("rbid");
    private static final ArrayField REPL_SLAVES_FIELD = new ArrayField("slaves");

    private final String setName;
    private final boolean ismaster;
    private final boolean secondary;
    private final String primary;
    private final ImmutableList<String> hosts;
    private final String me;
    private final BsonObjectId electionId;
    private final long rbid;
    private final ImmutableList<Slave> slaves;

    public Repl(String setName, boolean ismaster, boolean secondary, String primary,
        ImmutableList<String> hosts,
        String me, BsonObjectId electionId, long rbid, ImmutableList<Slave> slaves) {
      super();
      this.setName = setName;
      this.ismaster = ismaster;
      this.secondary = secondary;
      this.primary = primary;
      this.hosts = hosts;
      this.me = me;
      this.electionId = electionId;
      this.rbid = rbid;
      this.slaves = slaves;
    }

    private BsonDocument marshall() {
      BsonArrayBuilder hostsArr = new BsonArrayBuilder();
      for (String host : hosts) {
        hostsArr.add(host);
      }

      BsonArrayBuilder slavesArr = new BsonArrayBuilder();
      for (Slave slave : slaves) {
        slavesArr.add(slave.marshall());
      }

      return new BsonDocumentBuilder()
          .append(REPL_SET_NAME_FIELD, setName)
          .append(REPL_ISMASTER_FIELD, ismaster)
          .append(REPL_SECONDARY_FIELD, secondary)
          .append(REPL_PRIMARY_FIELD, primary)
          .append(REPL_HOSTS_FIELD, hostsArr.build())
          .append(REPL_ME_FIELD, me)
          .append(REPL_ELECTION_ID_FIELD, electionId)
          .append(REPL_RBID_FIELD, rbid)
          .append(REPL_SLAVES_FIELD, slavesArr.build())
          .build();
    }

    public String getSetName() {
      return setName;
    }

    public boolean isIsmaster() {
      return ismaster;
    }

    public boolean isSecondary() {
      return secondary;
    }

    public String getPrimary() {
      return primary;
    }

    public ImmutableList<String> getHosts() {
      return hosts;
    }

    public String getMe() {
      return me;
    }

    public BsonObjectId getElectionId() {
      return electionId;
    }

    public long getRbid() {
      return rbid;
    }

    public ImmutableList<Slave> getSlaves() {
      return slaves;
    }

    public static class Slave {

      private static final LongField REPL_SLAVES_RID_FIELD = new LongField("rid");
      private static final StringField REPL_SLAVES_HOST_FIELD = new StringField("host");
      private static final LongField REPL_SLAVES_OPTIME_FIELD = new LongField("optime");
      private static final LongField REPL_SLAVES_MEMBER_ID_FIELD = new LongField("memberID");

      private final long rid;
      private final String host;
      private final long optime;
      private final long memeberId;

      public Slave(long rid, String host, long optime, long memeberId) {
        super();
        this.rid = rid;
        this.host = host;
        this.optime = optime;
        this.memeberId = memeberId;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(REPL_SLAVES_RID_FIELD, rid)
            .append(REPL_SLAVES_HOST_FIELD, host)
            .append(REPL_SLAVES_OPTIME_FIELD, optime)
            .append(REPL_SLAVES_MEMBER_ID_FIELD, memeberId)
            .build();
      }

      public long getRid() {
        return rid;
      }

      public String getHost() {
        return host;
      }

      public long getOptime() {
        return optime;
      }

      public long getMemeberId() {
        return memeberId;
      }
    }
  }

  public static class Opcounters {

    private static final IntField OPCOUNTERS_INSERT_FIELD = new IntField("insert");
    private static final IntField OPCOUNTERS_QUERY_FIELD = new IntField("query");
    private static final IntField OPCOUNTERS_UPDATE_FIELD = new IntField("update");
    private static final IntField OPCOUNTERS_DELETE_FIELD = new IntField("delete");
    private static final IntField OPCOUNTERS_GETMORE_FIELD = new IntField("getmore");
    private static final IntField OPCOUNTERS_COMMAND_FIELD = new IntField("command");

    private final int insert;
    private final int query;
    private final int update;
    private final int delete;
    private final int getmore;
    private final int command;

    public Opcounters(int insert, int query, int update, int delete, int getmore,
        int command) {
      super();
      this.insert = insert;
      this.query = query;
      this.update = update;
      this.delete = delete;
      this.getmore = getmore;
      this.command = command;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(OPCOUNTERS_INSERT_FIELD, insert)
          .append(OPCOUNTERS_QUERY_FIELD, query)
          .append(OPCOUNTERS_UPDATE_FIELD, update)
          .append(OPCOUNTERS_DELETE_FIELD, delete)
          .append(OPCOUNTERS_GETMORE_FIELD, getmore)
          .append(OPCOUNTERS_COMMAND_FIELD, command)
          .build();
    }

    public int getInsert() {
      return insert;
    }

    public int getQuery() {
      return query;
    }

    public int getUpdate() {
      return update;
    }

    public int getDelete() {
      return delete;
    }

    public int getGetmore() {
      return getmore;
    }

    public int getCommand() {
      return command;
    }
  }

  public static class RangeDeleter {

    private static final ArrayField RANGE_DELETER_LAST_DELETE_STATS_FIELD = new ArrayField(
        "lastDeleteStats");

    private final ImmutableList<LastDeletedStat> lastDeleteStats;

    public RangeDeleter(ImmutableList<LastDeletedStat> lastDeleteStats) {
      super();
      this.lastDeleteStats = lastDeleteStats;
    }

    private BsonDocument marshall() {
      BsonArrayBuilder lastDeleteStatsArr = new BsonArrayBuilder();
      for (LastDeletedStat lastDeletedStat : lastDeleteStats) {
        lastDeleteStatsArr.add(lastDeletedStat.marshall());
      }

      return new BsonDocumentBuilder()
          .append(RANGE_DELETER_LAST_DELETE_STATS_FIELD, lastDeleteStatsArr.build())
          .build();
    }

    public ImmutableList<LastDeletedStat> getLastDeleteStats() {
      return lastDeleteStats;
    }

    public static class LastDeletedStat {

      private static final IntField RANGE_DELETER_LAST_DELETE_STATS_DELETED_DOCS_FIELD =
          new IntField("deletedDocs");
      private static final DateTimeField RANGE_DELETER_LAST_DELETE_STATS_QUEUE_START_FIELD =
          new DateTimeField("queueStart");
      private static final DateTimeField RANGE_DELETER_LAST_DELETE_STATS_QUEUE_END_FIELD =
          new DateTimeField("queueEnd");
      private static final DateTimeField RANGE_DELETER_LAST_DELETE_STATS_DELETE_START_FIELD =
          new DateTimeField("deleteStart");
      private static final DateTimeField RANGE_DELETER_LAST_DELETE_STATS_DELETE_END_FIELD =
          new DateTimeField("deleteEnd");
      private static final DateTimeField RANGE_DELETER_LAST_DELETE_STATS_WAIT_FOR_START_FIELD =
          new DateTimeField("waitForReplStart");
      private static final DateTimeField RANGE_DELETER_LAST_DELETE_STATS_WAIT_FOR_END_FIELD =
          new DateTimeField("waitForReplEnd");

      private final int deletedDocs;
      private final Instant queueStart;
      private final Instant queueEnd;
      private final Instant deleteStart;
      private final Instant deleteEnd;
      private final Instant waitForReplStart;
      private final Instant waitForReplEnd;

      public LastDeletedStat(int deletedDocs, Instant queueStart, Instant queueEnd,
          Instant deleteStart,
          Instant deleteEnd, Instant waitForReplStart, Instant waitForReplEnd) {
        super();
        this.deletedDocs = deletedDocs;
        this.queueStart = queueStart;
        this.queueEnd = queueEnd;
        this.deleteStart = deleteStart;
        this.deleteEnd = deleteEnd;
        this.waitForReplStart = waitForReplStart;
        this.waitForReplEnd = waitForReplEnd;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(RANGE_DELETER_LAST_DELETE_STATS_DELETED_DOCS_FIELD, deletedDocs)
            .append(RANGE_DELETER_LAST_DELETE_STATS_QUEUE_START_FIELD, queueStart)
            .append(RANGE_DELETER_LAST_DELETE_STATS_QUEUE_END_FIELD, queueEnd)
            .append(RANGE_DELETER_LAST_DELETE_STATS_DELETE_START_FIELD, deleteStart)
            .append(RANGE_DELETER_LAST_DELETE_STATS_DELETE_END_FIELD, deleteEnd)
            .append(RANGE_DELETER_LAST_DELETE_STATS_WAIT_FOR_START_FIELD, waitForReplStart)
            .append(RANGE_DELETER_LAST_DELETE_STATS_WAIT_FOR_END_FIELD, waitForReplEnd)
            .build();
      }

      public int getDeletedDocs() {
        return deletedDocs;
      }

      public Instant getQueueStart() {
        return queueStart;
      }

      public Instant getQueueEnd() {
        return queueEnd;
      }

      public Instant getDeleteStart() {
        return deleteStart;
      }

      public Instant getDeleteEnd() {
        return deleteEnd;
      }

      public Instant getWaitForReplStart() {
        return waitForReplStart;
      }

      public Instant getWaitForReplEnd() {
        return waitForReplEnd;
      }
    }
  }

  @SuppressWarnings("checkstyle:AbbreviationAsWordInName") //TODO: remove abbreviations
  public static class Security {

    private static final StringField SECURITY_SSL_SERVER_SUBJECT_NAME_FIELD = new StringField(
        "SSLServerSubjectName");
    private static final BooleanField SECURITY_SSL_SERVER_HAS_CERTIFICATE_AUTHORITY_FIELD =
        new BooleanField("SSLServerHasCertificateAuthority");
    private static final DateTimeField SECURITY_SSL_SERVER_CERTIFICATE_EXPIRATION_DATE_FIELD =
        new DateTimeField("SSLServerCertificateExpirationDate");

    private final String sslServerSubjectName;
    private final boolean sslServerHasCertificateAuthority;
    private final Instant sslServerCertificateExpirationDate;

    public Security(String sSLServerSubjectName, boolean sSLServerHasCertificateAuthority,
        Instant sSLServerCertificateExpirationDate) {
      super();
      sslServerSubjectName = sSLServerSubjectName;
      sslServerHasCertificateAuthority = sSLServerHasCertificateAuthority;
      sslServerCertificateExpirationDate = sSLServerCertificateExpirationDate;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(SECURITY_SSL_SERVER_SUBJECT_NAME_FIELD, sslServerSubjectName)
          .append(SECURITY_SSL_SERVER_HAS_CERTIFICATE_AUTHORITY_FIELD,
              sslServerHasCertificateAuthority)
          .append(SECURITY_SSL_SERVER_CERTIFICATE_EXPIRATION_DATE_FIELD,
              sslServerCertificateExpirationDate)
          .build();
    }

    public String getSSLServerSubjectName() {
      return sslServerSubjectName;
    }

    public boolean isSSLServerHasCertificateAuthority() {
      return sslServerHasCertificateAuthority;
    }

    public Instant getSSLServerCertificateExpirationDate() {
      return sslServerCertificateExpirationDate;
    }
  }

  public static class StorageEngine {

    private static final StringField STORAGE_ENGINE_NAME_FIELD = new StringField("name");

    private final String name;

    public StorageEngine(String name) {
      super();
      this.name = name;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(STORAGE_ENGINE_NAME_FIELD, name)
          .build();
    }

    public String getName() {
      return name;
    }
  }

  public static class Asserts {

    private static final IntField ASSERTS_REGULAR_FIELD = new IntField("regular");
    private static final IntField ASSERTS_WARNING_FIELD = new IntField("warning");
    private static final IntField ASSERTS_MSG_FIELD = new IntField("msg");
    private static final IntField ASSERTS_USER_FIELD = new IntField("user");
    private static final IntField ASSERTS_ROLLOVERS_FIELD = new IntField("rollovers");

    private final Integer regular;
    private final Integer warning;
    private final Integer msg;
    private final Integer user;
    private final Integer rollovers;

    public Asserts(Integer regular, Integer warning, Integer msg, Integer user, Integer rollovers) {
      super();
      this.regular = regular;
      this.warning = warning;
      this.msg = msg;
      this.user = user;
      this.rollovers = rollovers;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(ASSERTS_REGULAR_FIELD, regular)
          .append(ASSERTS_WARNING_FIELD, warning)
          .append(ASSERTS_MSG_FIELD, msg)
          .append(ASSERTS_USER_FIELD, user)
          .append(ASSERTS_ROLLOVERS_FIELD, rollovers)
          .build();
    }

    public Integer getRegular() {
      return regular;
    }

    public Integer getWarning() {
      return warning;
    }

    public Integer getMsg() {
      return msg;
    }

    public Integer getUser() {
      return user;
    }

    public Integer getRollovers() {
      return rollovers;
    }
  }

  public static class Dur {

    private static final IntField DUR_COMMITS_FIELD = new IntField("commits");
    private static final LongField DUR_JOURNALED_MB_FIELD = new LongField("journaledMB");
    private static final LongField DUR_WRITE_TO_DATA_FILES_MB_FIELD = new LongField(
        "writeToDataFilesMB");
    private static final IntField DUR_COMPRESSION_FIELD = new IntField("compression");
    private static final IntField DUR_COMMITS_IN_WRITE_LOCK_FIELD = new IntField(
        "commitsInWriteLock");
    private static final IntField DUR_EARLY_COMMITS_FIELD = new IntField("earlyCommits");
    private static final DocField DUR_TIME_MS_FIELD = new DocField("timeMS");

    private final int commits;
    private final long journaledMb;
    private final long writeToDataFilesMb;
    private final int compression;
    private final int commitsInWriteLock;
    private final int earlyCommits;
    private final TimeMs timeMs;

    public Dur(int commits, long journaledMb, long writeToDataFilesMb, int compression,
        int commitsInWriteLock, int earlyCommits, TimeMs timeMs) {
      super();
      this.commits = commits;
      this.journaledMb = journaledMb;
      this.writeToDataFilesMb = writeToDataFilesMb;
      this.compression = compression;
      this.commitsInWriteLock = commitsInWriteLock;
      this.earlyCommits = earlyCommits;
      this.timeMs = timeMs;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(DUR_COMMITS_FIELD, commits)
          .append(DUR_JOURNALED_MB_FIELD, journaledMb)
          .append(DUR_WRITE_TO_DATA_FILES_MB_FIELD, writeToDataFilesMb)
          .append(DUR_COMPRESSION_FIELD, compression)
          .append(DUR_COMMITS_IN_WRITE_LOCK_FIELD, commitsInWriteLock)
          .append(DUR_EARLY_COMMITS_FIELD, earlyCommits)
          .append(DUR_TIME_MS_FIELD, timeMs.marshall())
          .build();
    }

    public int getCommits() {
      return commits;
    }

    public long getJournaledMb() {
      return journaledMb;
    }

    public long getWriteToDataFilesMb() {
      return writeToDataFilesMb;
    }

    public int getCompression() {
      return compression;
    }

    public int getCommitsInWriteLock() {
      return commitsInWriteLock;
    }

    public int getEarlyCommits() {
      return earlyCommits;
    }

    public TimeMs getTimeMs() {
      return timeMs;
    }

    public static class TimeMs {

      private static final LongField DUR_TIME_MS_DT_FIELD = new LongField("dt");
      private static final LongField DUR_TIME_MS_PREP_LOG_BUFFER_FIELD = new LongField(
          "prepLogBuffer");
      private static final LongField DUR_TIME_MS_WRITE_TO_JOURNAL_FIELD = new LongField(
          "writeToJournal");
      private static final LongField DUR_TIME_MS_WRITE_DATA_FILES_FIELD = new LongField(
          "writeToDataFiles");
      private static final LongField DUR_TIME_MS_REMAP_PRIVATE_VIEW_FIELD = new LongField(
          "remapPrivateView");
      private static final IntField DUR_TIME_MS_COMMITS_FIELD = new IntField("commits");
      private static final IntField DUR_TIME_MS_COMMITS_IN_WRITE_LOCK_FIELD = new IntField(
          "commitsInWriteLock");

      private final long dt;
      private final long prepLogBuffer;
      private final long writeToJournal;
      private final long writeToDataFiles;
      private final long remapPrivateView;
      private final int commits;
      private final int commitsInWriteLock;

      public TimeMs(long dt, long prepLogBuffer, long writeToJournal, long writeToDataFiles,
          long remapPrivateView, int commits, int commitsInWriteLock) {
        super();
        this.dt = dt;
        this.prepLogBuffer = prepLogBuffer;
        this.writeToJournal = writeToJournal;
        this.writeToDataFiles = writeToDataFiles;
        this.remapPrivateView = remapPrivateView;
        this.commits = commits;
        this.commitsInWriteLock = commitsInWriteLock;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(DUR_TIME_MS_DT_FIELD, dt)
            .append(DUR_TIME_MS_PREP_LOG_BUFFER_FIELD, prepLogBuffer)
            .append(DUR_TIME_MS_WRITE_TO_JOURNAL_FIELD, writeToJournal)
            .append(DUR_TIME_MS_WRITE_DATA_FILES_FIELD, writeToDataFiles)
            .append(DUR_TIME_MS_REMAP_PRIVATE_VIEW_FIELD, remapPrivateView)
            .append(DUR_TIME_MS_COMMITS_FIELD, commits)
            .append(DUR_TIME_MS_COMMITS_IN_WRITE_LOCK_FIELD, commitsInWriteLock)
            .build();
      }

      public long getDt() {
        return dt;
      }

      public long getPrepLogBuffer() {
        return prepLogBuffer;
      }

      public long getWriteToJournal() {
        return writeToJournal;
      }

      public long getWriteToDataFiles() {
        return writeToDataFiles;
      }

      public long getRemapPrivateView() {
        return remapPrivateView;
      }

      public int getCommits() {
        return commits;
      }

      public int getCommitsInWriteLock() {
        return commitsInWriteLock;
      }
    }
  }

  public static class Metrics {

    private static final ArrayField METRICS_COMMANDS_FIELD = new ArrayField("commands");
    private static final DocField METRICS_DOCUMENT_FIELD = new DocField("document");
    private static final DocField METRICS_GET_LAST_ERROR_FIELD = new DocField("getLastError");
    private static final DocField METRICS_OPERATION_FIELD = new DocField("operation");
    private static final DocField METRICS_QUERY_EXECUTOR_FIELD = new DocField("queryExecutor");
    private static final DocField METRICS_RECORD_FIELD = new DocField("record");
    private static final DocField METRICS_REPL_FIELD = new DocField("repl");
    private static final DocField METRICS_STORAGE_FIELD = new DocField("storage");
    private static final DocField METRICS_TTL_FIELD = new DocField("ttl");

    private final ImmutableList<Command> commands;
    private final Document document;
    private final GetLastError getLastError;
    private final Operation operation;
    private final QueryExecutor queryExecutor;
    private final Record record;
    private final Repl repl;
    private final Storage storage;
    private final Ttl ttl;

    public Metrics(ImmutableList<Command> commands, Document document, GetLastError getLastError,
        Operation operation, QueryExecutor queryExecutor, Record record, Repl repl, Storage storage,
        Ttl ttl) {
      super();
      this.commands = commands;
      this.document = document;
      this.getLastError = getLastError;
      this.operation = operation;
      this.queryExecutor = queryExecutor;
      this.record = record;
      this.repl = repl;
      this.storage = storage;
      this.ttl = ttl;
    }

    private BsonDocument marshall() {
      BsonArrayBuilder commandsArr = new BsonArrayBuilder();
      for (Command command : commands) {
        commandsArr.add(command.marshall());
      }

      return new BsonDocumentBuilder()
          .append(METRICS_COMMANDS_FIELD, commandsArr.build())
          .append(METRICS_DOCUMENT_FIELD, document.marshall())
          .append(METRICS_GET_LAST_ERROR_FIELD, getLastError.marshall())
          .append(METRICS_OPERATION_FIELD, operation.marshall())
          .append(METRICS_QUERY_EXECUTOR_FIELD, queryExecutor.marshall())
          .append(METRICS_RECORD_FIELD, record.marshall())
          .append(METRICS_REPL_FIELD, repl.marshall())
          .append(METRICS_STORAGE_FIELD, storage.marshall())
          .append(METRICS_TTL_FIELD, ttl.marshall())
          .build();
    }

    public ImmutableList<Command> getCommands() {
      return commands;
    }

    public Document getDocument() {
      return document;
    }

    public GetLastError getGetLastError() {
      return getLastError;
    }

    public Operation getOperation() {
      return operation;
    }

    public QueryExecutor getQueryExecutor() {
      return queryExecutor;
    }

    public Record getRecord() {
      return record;
    }

    public Repl getRepl() {
      return repl;
    }

    public Storage getStorage() {
      return storage;
    }

    public Ttl getTtl() {
      return ttl;
    }

    public static class Command {

      private static final IntField METRICS_COMMANDS_FAILED_FIELD = new IntField("failed");
      private static final IntField METRICS_COMMANDS_TOTAL_FIELD = new IntField("total");

      private final String command;
      private final int failed;
      private final int total;

      public Command(String command, int failed, int total) {
        super();
        this.command = command;
        this.failed = failed;
        this.total = total;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .appendUnsafe(command,
                new BsonDocumentBuilder()
                    .append(METRICS_COMMANDS_FAILED_FIELD, failed)
                    .append(METRICS_COMMANDS_TOTAL_FIELD, total).build())
            .build();
      }

      public String getCommand() {
        return command;
      }

      public int getFailed() {
        return failed;
      }

      public int getTotal() {
        return total;
      }
    }

    public static class Document {

      private static final IntField METRICS_DOCUMENT_DELETED_FIELD = new IntField("deleted");
      private static final IntField METRICS_DOCUMENT_INSERTED_FIELD = new IntField("inserted");
      private static final IntField METRICS_DOCUMENT_RETURNED_FIELD = new IntField("returned");
      private static final IntField METRICS_DOCUMENT_UPDATED_FIELD = new IntField("updated");

      private final int deleted;
      private final int inserted;
      private final int returned;
      private final int updated;

      public Document(int deleted, int inserted, int returned, int updated) {
        super();
        this.deleted = deleted;
        this.inserted = inserted;
        this.returned = returned;
        this.updated = updated;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_DOCUMENT_DELETED_FIELD, deleted)
            .append(METRICS_DOCUMENT_INSERTED_FIELD, inserted)
            .append(METRICS_DOCUMENT_RETURNED_FIELD, returned)
            .append(METRICS_DOCUMENT_UPDATED_FIELD, updated)
            .build();
      }

      public int getDeleted() {
        return deleted;
      }

      public int getInserted() {
        return inserted;
      }

      public int getReturned() {
        return returned;
      }

      public int getUpdated() {
        return updated;
      }
    }

    public static class GetLastError {

      private static final DocField METRICS_GET_LAST_ERROR_WTIME_FIELD = new DocField("wtime");
      private static final IntField METRICS_GET_LAST_ERROR_WTIMEOUTS_FIELD = new IntField(
          "wtimeouts");

      private final Stats wtime;
      private final int wtimeouts;

      public GetLastError(Stats wtime, int wtimeouts) {
        super();
        this.wtime = wtime;
        this.wtimeouts = wtimeouts;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_GET_LAST_ERROR_WTIME_FIELD, wtime.marshall())
            .append(METRICS_GET_LAST_ERROR_WTIMEOUTS_FIELD, wtimeouts)
            .build();
      }

      public Stats getWtime() {
        return wtime;
      }

      public int getWtimeouts() {
        return wtimeouts;
      }
    }

    public static class Operation {

      private static final IntField METRICS_OPERATION_FASTMOD_FIELD = new IntField("fastmod");
      private static final IntField METRICS_OPERATION_IDHACK_FIELD = new IntField("idhack");
      private static final IntField METRICS_OPERATION_SCAN_AND_ORDER_FIELD = new IntField(
          "scanAndOrder");

      private final int fastmod;
      private final int idhack;
      private final int scanAndOrder;

      public Operation(int fastmod, int idhack, int scanAndOrder) {
        super();
        this.fastmod = fastmod;
        this.idhack = idhack;
        this.scanAndOrder = scanAndOrder;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_OPERATION_FASTMOD_FIELD, fastmod)
            .append(METRICS_OPERATION_IDHACK_FIELD, idhack)
            .append(METRICS_OPERATION_SCAN_AND_ORDER_FIELD, scanAndOrder)
            .build();
      }

      public int getFastmod() {
        return fastmod;
      }

      public int getIdhack() {
        return idhack;
      }

      public int getScanAndOrder() {
        return scanAndOrder;
      }
    }

    public static class QueryExecutor {

      private static final IntField METRICS_QUERY_EXECUTOR_SCANNED_FIELD = new IntField("scanned");

      private final int scanned;

      public QueryExecutor(int scanned) {
        super();
        this.scanned = scanned;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_QUERY_EXECUTOR_SCANNED_FIELD, scanned)
            .build();
      }

      public int getScanned() {
        return scanned;
      }
    }

    public static class Record {

      private static final IntField METRICS_RECORD_MOVES_FIELD = new IntField("moves");

      private final int moves;

      public Record(int moves) {
        super();
        this.moves = moves;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_RECORD_MOVES_FIELD, moves)
            .build();
      }

      public int getMoves() {
        return moves;
      }
    }

    public static class Repl {

      private static final DocField METRICS_REPL_APPLY_FIELD = new DocField("apply");
      private static final DocField METRICS_REPL_BUFFER_FIELD = new DocField("buffer");
      private static final DocField METRICS_REPL_NETWORK_FIELD = new DocField("network");
      private static final DocField METRICS_REPL_OPLOG_FIELD = new DocField("oplog");
      private static final DocField METRICS_REPL_PRELOAD_FIELD = new DocField("preload");

      private final Apply apply;
      private final Buffer buffer;
      private final Network network;
      private final Oplog oplog;
      private final Preload preload;

      public Repl(Apply apply, Buffer buffer, Network network, Oplog oplog, Preload preload) {
        super();
        this.apply = apply;
        this.buffer = buffer;
        this.network = network;
        this.oplog = oplog;
        this.preload = preload;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_REPL_APPLY_FIELD, apply.marshall())
            .append(METRICS_REPL_BUFFER_FIELD, buffer.marshall())
            .append(METRICS_REPL_NETWORK_FIELD, network.marshall())
            .append(METRICS_REPL_OPLOG_FIELD, oplog.marshall())
            .append(METRICS_REPL_PRELOAD_FIELD, preload.marshall())
            .build();
      }

      public Apply getApply() {
        return apply;
      }

      public Buffer getBuffer() {
        return buffer;
      }

      public Network getNetwork() {
        return network;
      }

      public Oplog getOplog() {
        return oplog;
      }

      public Preload getPreload() {
        return preload;
      }

      public static class Apply {

        private static final DocField METRICS_REPL_APPLY_BATCHES_FIELD = new DocField("batches");
        private static final IntField METRICS_REPL_APPLY_OPS_FIELD = new IntField("ops");

        private final Stats batches;
        private final int ops;

        public Apply(Stats batches, int ops) {
          super();
          this.batches = batches;
          this.ops = ops;
        }

        private BsonDocument marshall() {
          return new BsonDocumentBuilder()
              .append(METRICS_REPL_APPLY_BATCHES_FIELD, batches.marshall())
              .append(METRICS_REPL_APPLY_OPS_FIELD, ops)
              .build();
        }

        public Stats getBatches() {
          return batches;
        }

        public int getOps() {
          return ops;
        }
      }

      public static class Buffer {

        private static final IntField METRICS_REPL_BUFFER_COUNT_FIELD = new IntField("count");
        private static final LongField METRICS_REPL_BUFFER_MAX_SIZE_BYTES_FIELD = new LongField(
            "maxSizeBytes");
        private static final LongField METRICS_REPL_BUFFER_SIZE_BYTES_FIELD = new LongField(
            "sizeBytes");

        private final int count;
        private final long maxSizeBytes;
        private final long sizeBytes;

        public Buffer(int count, long maxSizeBytes, long sizeBytes) {
          super();
          this.count = count;
          this.maxSizeBytes = maxSizeBytes;
          this.sizeBytes = sizeBytes;
        }

        private BsonDocument marshall() {
          return new BsonDocumentBuilder()
              .append(METRICS_REPL_BUFFER_COUNT_FIELD, count)
              .append(METRICS_REPL_BUFFER_MAX_SIZE_BYTES_FIELD, maxSizeBytes)
              .append(METRICS_REPL_BUFFER_SIZE_BYTES_FIELD, sizeBytes)
              .build();
        }

        public int getCount() {
          return count;
        }

        public long getMaxSizeBytes() {
          return maxSizeBytes;
        }

        public long getSizeBytes() {
          return sizeBytes;
        }
      }

      public static class Network {

        private static final LongField METRICS_REPL_NETWORK_BYTES_FIELD = new LongField("bytes");
        @SuppressWarnings("checkstyle:LineLength")
        private static final DocField METRICS_REPL_NETWORK_GETMORES_FIELD = new DocField("getmores");
        private static final IntField METRICS_REPL_NETWORK_OPS_FIELD = new IntField("ops");
        private static final IntField METRICS_REPL_NETWORK_READERS_CREATED_FIELD = new IntField(
            "readersCreated");

        private final long bytes;
        private final Stats getmores;
        private final int ops;
        private final int readersCreated;

        public Network(long bytes, Stats getmores, int ops, int readersCreated) {
          super();
          this.bytes = bytes;
          this.getmores = getmores;
          this.ops = ops;
          this.readersCreated = readersCreated;
        }

        private BsonDocument marshall() {
          return new BsonDocumentBuilder()
              .append(METRICS_REPL_NETWORK_BYTES_FIELD, bytes)
              .append(METRICS_REPL_NETWORK_GETMORES_FIELD, getmores.marshall())
              .append(METRICS_REPL_NETWORK_OPS_FIELD, ops)
              .append(METRICS_REPL_NETWORK_READERS_CREATED_FIELD, readersCreated)
              .build();
        }

        public long getBytes() {
          return bytes;
        }

        public Stats getGetmores() {
          return getmores;
        }

        public int getOps() {
          return ops;
        }

        public int getReadersCreated() {
          return readersCreated;
        }
      }

      public static class Oplog {

        private static final DocField METRICS_REPL_OPLOG_INSERT_FIELD = new DocField("insert");
        private static final LongField METRICS_REPL_OPLOG_INSERT_BYTES_FIELD = new LongField(
            "insertBytes");

        private final Stats insert;
        private final long insertBytes;

        public Oplog(Stats insert, long insertBytes) {
          super();
          this.insert = insert;
          this.insertBytes = insertBytes;
        }

        private BsonDocument marshall() {
          return new BsonDocumentBuilder()
              .append(METRICS_REPL_OPLOG_INSERT_FIELD, insert.marshall())
              .append(METRICS_REPL_OPLOG_INSERT_BYTES_FIELD, insertBytes)
              .build();
        }

        public Stats getInsert() {
          return insert;
        }

        public long getInsertBytes() {
          return insertBytes;
        }
      }

      public static class Preload {

        private static final DocField METRICS_REPL_PRELOAD_DOCS_FIELD = new DocField("docs");
        private static final DocField METRICS_REPL_PRELOAD_INDEXES_FIELD = new DocField("indexes");

        private final Stats docs;
        private final Stats indexes;

        public Preload(Stats docs, Stats indexes) {
          super();
          this.docs = docs;
          this.indexes = indexes;
        }

        private BsonDocument marshall() {
          return new BsonDocumentBuilder()
              .append(METRICS_REPL_PRELOAD_DOCS_FIELD, docs.marshall())
              .append(METRICS_REPL_PRELOAD_INDEXES_FIELD, indexes.marshall())
              .build();
        }

        public Stats getDocs() {
          return docs;
        }

        public Stats getIndexes() {
          return indexes;
        }
      }
    }

    public static class Storage {

      private static final DocField METRICS_STORAGE_FREELIST_FIELD = new DocField("freelist");

      private final Freelist freelist;

      public Storage(Freelist freelist) {
        super();
        this.freelist = freelist;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_STORAGE_FREELIST_FIELD, freelist.marshall())
            .build();
      }

      public Freelist getFreelist() {
        return freelist;
      }

      public static class Freelist {

        @SuppressWarnings("checkstyle:LineLength")
        private static final DocField METRICS_STORAGE_FREELIST_SEARCH_FIELD = new DocField("search");

        private final Search search;

        public Freelist(Search search) {
          super();
          this.search = search;
        }

        private BsonDocument marshall() {
          return new BsonDocumentBuilder()
              .append(METRICS_STORAGE_FREELIST_SEARCH_FIELD, search.marshall())
              .build();
        }

        public Search getSearch() {
          return search;
        }

        public static class Search {

          private static final IntField METRICS_STORAGE_FREELIST_SEARCH_BUCKET_EXHAUSTED_FIELD =
              new IntField("bucketExhausted");
          private static final IntField METRICS_STORAGE_FREELIST_SEARCH_REQUESTS_FIELD =
              new IntField("requests");
          private static final IntField METRICS_STORAGE_FREELIST_SEARCH_SCANNED_FIELD =
              new IntField("scanned");

          private final int bucketExhausted;
          private final int requests;
          private final int scanned;

          public Search(int bucketExhausted, int requests, int scanned) {
            super();
            this.bucketExhausted = bucketExhausted;
            this.requests = requests;
            this.scanned = scanned;
          }

          private BsonDocument marshall() {
            return new BsonDocumentBuilder()
                .append(METRICS_STORAGE_FREELIST_SEARCH_BUCKET_EXHAUSTED_FIELD, bucketExhausted)
                .append(METRICS_STORAGE_FREELIST_SEARCH_REQUESTS_FIELD, requests)
                .append(METRICS_STORAGE_FREELIST_SEARCH_SCANNED_FIELD, scanned)
                .build();
          }

          public int getBucketExhausted() {
            return bucketExhausted;
          }

          public int getRequests() {
            return requests;
          }

          public int getScanned() {
            return scanned;
          }
        }
      }
    }

    public static class Ttl {

      private static final IntField METRICS_TTL_DELETED_DOCUMENTS_FIELD = new IntField(
          "deletedDocuments");
      private static final IntField METRICS_TTL_PASSES_FIELD = new IntField("passes");

      private final int deletedDocuments;
      private final int passes;

      public Ttl(int deletedDocuments, int passes) {
        super();
        this.deletedDocuments = deletedDocuments;
        this.passes = passes;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_TTL_DELETED_DOCUMENTS_FIELD, deletedDocuments)
            .append(METRICS_TTL_PASSES_FIELD, passes)
            .build();
      }

      public int getDeletedDocuments() {
        return deletedDocuments;
      }

      public int getPasses() {
        return passes;
      }
    }

    public static class Stats {

      private static final IntField METRICS_NUM_FIELD = new IntField("num");
      private static final LongField METRICS_TOTAL_MILLIS_FIELD = new LongField("totalMillis");

      private final int num;
      private final long totalMillis;

      public Stats(int num, long totalMillis) {
        super();
        this.num = num;
        this.totalMillis = totalMillis;
      }

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .append(METRICS_NUM_FIELD, num)
            .append(METRICS_TOTAL_MILLIS_FIELD, totalMillis)
            .build();
      }

      public int getNum() {
        return num;
      }

      public long getTotalMillis() {
        return totalMillis;
      }
    }
  }

  public static class WiredTiger {

    private static final StringField WIRED_TIGER_URI_FIELD = new StringField("uri");
    private static final DocField WIRED_TIGER_LSM_FIELD = new DocField("LSM");
    private static final DocField WIRED_TIGER_ASYNC_FIELD = new DocField("async");
    private static final DocField WIRED_TIGER_BLOCK_MANAGER_FIELD = new DocField("block-manager");
    private static final DocField WIRED_TIGER_CACHE_FIELD = new DocField("cache");
    private static final DocField WIRED_TIGER_CONNECTION_FIELD = new DocField("connection");
    private static final DocField WIRED_TIGER_CURSOR_FIELD = new DocField("cursor");
    private static final DocField WIRED_TIGER_DATA_HANDLE_FIELD = new DocField("data-handle");
    private static final DocField WIRED_TIGER_LOG_FIELD = new DocField("log");
    private static final DocField WIRED_TIGER_RECONCILIATION_FIELD = new DocField("reconciliation");
    private static final DocField WIRED_TIGER_SESSION_FIELD = new DocField("session");
    private static final DocField WIRED_TIGER_THREAD_YIELD_FIELD = new DocField("thread-yield");
    private static final DocField WIRED_TIGER_TRANSACTION_FIELD = new DocField("transaction");
    private static final DocField WIRED_TIGER_CONCURRENT_TRANSACTIONS_FIELD = new DocField(
        "concurrentTransactions");

    private final String uri;
    private final Lsm lsm;
    private final Async async;
    private final BlockManager blockManager;
    private final Cache cache;
    private final Connection connection;
    private final Cursor cursor;
    private final DataHandle dataHandle;
    private final Log log;
    private final Reconciliation reconciliation;
    private final Session session;
    private final ThreadYield threadYield;
    private final Transaction transaction;
    private final ConcurrentTransaction concurrentTransactions;

    public WiredTiger(String uri, Lsm lsm, Async async, BlockManager blockManager, Cache cache,
        Connection connection, Cursor cursor, DataHandle dataHandle, Log log,
        Reconciliation reconciliation,
        Session session, ThreadYield threadYield, Transaction transaction,
        ConcurrentTransaction concurrentTransactions) {
      super();
      this.uri = uri;
      this.lsm = lsm;
      this.async = async;
      this.blockManager = blockManager;
      this.cache = cache;
      this.connection = connection;
      this.cursor = cursor;
      this.dataHandle = dataHandle;
      this.log = log;
      this.reconciliation = reconciliation;
      this.session = session;
      this.threadYield = threadYield;
      this.transaction = transaction;
      this.concurrentTransactions = concurrentTransactions;
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(WIRED_TIGER_URI_FIELD, uri)
          .append(WIRED_TIGER_LSM_FIELD, lsm.marshall())
          .append(WIRED_TIGER_ASYNC_FIELD, async.marshall())
          .append(WIRED_TIGER_BLOCK_MANAGER_FIELD, blockManager.marshall())
          .append(WIRED_TIGER_CACHE_FIELD, cache.marshall())
          .append(WIRED_TIGER_CONNECTION_FIELD, connection.marshall())
          .append(WIRED_TIGER_CURSOR_FIELD, cursor.marshall())
          .append(WIRED_TIGER_DATA_HANDLE_FIELD, dataHandle.marshall())
          .append(WIRED_TIGER_LOG_FIELD, log.marshall())
          .append(WIRED_TIGER_RECONCILIATION_FIELD, reconciliation.marshall())
          .append(WIRED_TIGER_SESSION_FIELD, session.marshall())
          .append(WIRED_TIGER_THREAD_YIELD_FIELD, threadYield.marshall())
          .append(WIRED_TIGER_TRANSACTION_FIELD, transaction.marshall())
          .append(WIRED_TIGER_CONCURRENT_TRANSACTIONS_FIELD, concurrentTransactions.marshall())
          .build();
    }

    public String getUri() {
      return uri;
    }

    public Lsm getLsm() {
      return lsm;
    }

    public Async getAsync() {
      return async;
    }

    public BlockManager getBlockManager() {
      return blockManager;
    }

    public Cache getCache() {
      return cache;
    }

    public Connection getConnection() {
      return connection;
    }

    public Cursor getCursor() {
      return cursor;
    }

    public DataHandle getDataHandle() {
      return dataHandle;
    }

    public Log getLog() {
      return log;
    }

    public Reconciliation getReconciliation() {
      return reconciliation;
    }

    public Session getSession() {
      return session;
    }

    public ThreadYield getThreadYield() {
      return threadYield;
    }

    public Transaction getTransaction() {
      return transaction;
    }

    public ConcurrentTransaction getConcurrentTransactions() {
      return concurrentTransactions;
    }

    //TODO: Complete
    /*
     * "LSM" : { "sleep for LSM checkpoint throttle" : <num>, "sleep for LSM merge throttle" :
     * <num>, "rows merged in an LSM tree" : <num>, "application work units currently queued" :
     * <num>, "merge work units currently queued" : <num>, "tree queue hit maximum" : <num>, "switch
     * work units currently queued" : <num>, "tree maintenance operations scheduled" : <num>, "tree
     * maintenance operations discarded" : <num>, "tree maintenance operations executed" : <num> },
     */
    public static class Lsm {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "async" : { "number of allocation state races" : <num>, "number of operation slots viewed for
     * allocation" : <num>, "current work queue length" : <num>, "number of flush calls" : <num>,
     * "number of times operation allocation failed" : <num>, "maximum work queue length" : <num>,
     * "number of times worker found no work" : <num>, "total allocations" : <num>, "total compact
     * calls" : <num>, "total insert calls" : <num>, "total remove calls" : <num>, "total search
     * calls" : <num>, "total update calls" : <num> },
     */
    public static class Async {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "block-manager" : { "mapped bytes read" : <num>, "bytes read" : <num>, "bytes written" :
     * <num>, "mapped blocks read" : <num>, "blocks pre-loaded" : <num>, "blocks read" : <num>,
     * "blocks written" : <num> },
     */
    public static class BlockManager {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "cache" : { "tracked dirty bytes in the cache" : <num>, "bytes currently in the cache" :
     * <num>, "maximum bytes configured" : <num>, "bytes read into cache" : <num>, "bytes written
     * from cache" : <num>, "pages evicted by application threads" : <num>, "checkpoint blocked page
     * eviction" : <num>, "unmodified pages evicted" : <num>, "page split during eviction deepened
     * the tree" : <num>, "modified pages evicted" : <num>, "pages selected for eviction unable to
     * be evicted" : <num>, "pages evicted because they exceeded the in-memory maximum" : <num>,
     * "pages evicted because they had chains of deleted items" : <num>, "failed eviction of pages
     * that exceeded the in-memory maximum" : <num>, "hazard pointer blocked page eviction" : <num>,
     * "internal pages evicted" : <num>, "maximum page size at eviction" : <num>, "eviction server
     * candidate queue empty when topping up" : <num>, "eviction server candidate queue not empty
     * when topping up" : <num>, "eviction server evicting pages" : <num>, "eviction server
     * populating queue, but not evicting pages" : <num>, "eviction server unable to reach eviction
     * goal" : <num>, "pages split during eviction" : <num>, "pages walked for eviction" : <num>,
     * "eviction worker thread evicting pages" : <num>, "in-memory page splits" : <num>, "percentage
     * overhead" : <num>, "tracked dirty pages in the cache" : <num>, "pages currently held in the
     * cache" : <num>, "pages read into cache" : <num>, "pages written from cache" : <num> },
     */
    public static class Cache {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "connection" : { "pthread mutex condition wait calls" : <num>, "files currently open" :
     * <num>, "memory allocations" : <num>, "memory frees" : <num>, "memory re-allocations" : <num>,
     * "total read I/Os" : <num>, "pthread mutex shared lock read-lock calls" : <num>, "pthread
     * mutex shared lock write-lock calls" : <num>, "total write I/Os" : <num> },
     */
    public static class Connection {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "cursor" : { "cursor create calls" : <num>, "cursor insert calls" : <num>, "cursor next
     * calls" : <num>, "cursor prev calls" : <num>, "cursor remove calls" : <num>, "cursor reset
     * calls" : <num>, "cursor search calls" : <num>, "cursor search near calls" : <num>, "cursor
     * update calls" : <num> },
     */
    public static class Cursor {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "data-handle" : { "connection dhandles swept" : <num>, "connection candidate referenced" :
     * <num>, "connection sweeps" : <num>, "connection time-of-death sets" : <num>, "session
     * dhandles swept" : <num>, "session sweep attempts" : <num> },
     */
    public static class DataHandle {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "log" : { "log buffer size increases" : <num>, "total log buffer size" : <num>, "log bytes of
     * payload data" : <num>, "log bytes written" : <num>, "yields waiting for previous log file
     * close" : <num>, "total size of compressed records" : <num>, "total in-memory size of
     * compressed records" : <num>, "log records too small to compress" : <num>, "log records not
     * compressed" : <num>, "log records compressed" : <num>, "maximum log file size" : <num>,
     * "pre-allocated log files prepared" : <num>, "number of pre-allocated log files to create" :
     * <num>, "pre-allocated log files used" : <num>, "log read operations" : <num>, "log release
     * advances write LSN" : <num>, "records processed by log scan" : <num>, "log scan records
     * requiring two reads" : <num>, "log scan operations" : <num>, "consolidated slot closures" :
     * <num>, "logging bytes consolidated" : <num>, "consolidated slot joins" : <num>, "consolidated
     * slot join races" : <num>, "slots selected for switching that were unavailable" : <num>,
     * "record size exceeded maximum" : <num>, "failed to find a slot large enough for record" :
     * <num>, "consolidated slot join transitions" : <num>, "log sync operations" : <num>, "log
     * sync_dir operations" : <num>, "log server thread advances write LSN" : <num>, "log write
     * operations" : <num> },
     */
    public static class Log {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "reconciliation" : { "page reconciliation calls" : <num>, "page reconciliation calls for
     * eviction" : <num>, "split bytes currently awaiting free" : <num>, "split objects currently
     * awaiting free" : <num> },
     */
    public static class Reconciliation {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "session" : { "open cursor count" : <num>, "open session count" : <num> },
     */
    public static class Session {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "thread-yield" : { "page acquire busy blocked" : <num>, "page acquire eviction blocked" :
     * <num>, "page acquire locked blocked" : <num>, "page acquire read blocked" : <num>, "page
     * acquire time sleeping (usecs)" : <num> },
     */
    public static class ThreadYield {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "transaction" : { "transaction begins" : <num>, "transaction checkpoints" : <num>,
     * "transaction checkpoint currently running" : <num>, "transaction checkpoint max time (msecs)"
     * : <num>, "transaction checkpoint min time (msecs)" : <num>, "transaction checkpoint most
     * recent time (msecs)" : <num>, "transaction checkpoint total time (msecs)" : <num>,
     * "transactions committed" : <num>, "transaction failures due to cache overflow" : <num>,
     * "transaction range of IDs currently pinned" : <num>, "transactions rolled back" : <num> },
     */
    public static class Transaction {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }

    /*
     * "concurrentTransactions" : { "write" : { "out" : <num>, "available" : <num>, "totalTickets" :
     * <num> }, "read" : { "out" : <num>, "available" : <num>, "totalTickets" : <num> } }
     */
    public static class ConcurrentTransaction {

      private BsonDocument marshall() {
        return new BsonDocumentBuilder()
            .build();
      }
    }
  }

}
