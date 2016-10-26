
package com.torodb.mongodb.commands.impl.diagnostic;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.time.Instant;

import javax.inject.Inject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Asserts;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.BackgroundFlushing;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Connections;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Cursors;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Dur;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.ExtraInfo;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.GlobalLock;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Locks;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Mem;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Metrics;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Network;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Opcounters;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.RangeDeleter;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.Security;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.ServerStatusArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.ServerStatusReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.StorageEngine;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.impl.ConnectionTorodbCommandImpl;
import com.torodb.mongodb.core.MongoLayerConstants;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServerConfig;

/**
 *
 */
public class ServerStatusImplementation extends ConnectionTorodbCommandImpl<ServerStatusArgument, ServerStatusReply>{

    private static final Logger LOGGER
            = LogManager.getLogger(ServerStatusImplementation.class);

    private final HostAndPort selfHostAndPort;

    @Inject
    public ServerStatusImplementation(MongodServerConfig mongoServerConfig) {
        selfHostAndPort = mongoServerConfig.getHostAndPort();
    }


    @Override
    public Status<ServerStatusReply> apply(Request req,
            Command<? super ServerStatusArgument, ? super ServerStatusReply> command, ServerStatusArgument arg,
            MongodConnection context) {
        ServerStatusReply.Builder replyBuilder = new ServerStatusReply.Builder();
        
        //TODO: improve and complete
        
        if (arg.isHost()) {
            try {
                replyBuilder.setHost(InetAddress.getLocalHost().getHostName() + ":" + selfHostAndPort.getPort());
            } catch(Throwable throwable) {
                replyBuilder.setHost("localhost:" + selfHostAndPort.getPort());
            }
        }
        if (arg.isVersion()) replyBuilder.setVersion(MongoLayerConstants.VERSION_STRING);
        if (arg.isProcess()) replyBuilder.setProcess("mongod");
        if (arg.isPid()) {
            try {
                replyBuilder.setPid(Integer.valueOf(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]));
            } catch(Throwable throwable) {
                LOGGER.warn("Cannot get PID: " + throwable.getMessage());
            }
        }
        if (arg.isUptime()) replyBuilder.setUptime(ManagementFactory.getRuntimeMXBean().getUptime());
        if (arg.isUptimeEstimate()) replyBuilder.setUptimeEstimate(ManagementFactory.getRuntimeMXBean().getUptime());
        if (arg.isLocalTime()) replyBuilder.setLocalTime(Instant.now());
        if (arg.isLocks()) {
            Locks.Count dummyCount = new Locks.Count(0, 0, 0, 0);
            Locks.Lock dummyLock = new Locks.Lock(dummyCount, dummyCount, dummyCount, dummyCount);
            replyBuilder.setLocks(new Locks(dummyLock, dummyLock, dummyLock, dummyLock, dummyLock, dummyLock));
        }
        if (arg.isGlobalLock()) {
            GlobalLock.GlobalLockStats dummyGlobalLockStats = new GlobalLock.GlobalLockStats(0, 0, 0);
            replyBuilder.setGlobalLock(new GlobalLock(0, dummyGlobalLockStats, dummyGlobalLockStats));
        }
        if (arg.isMem()) {
            replyBuilder.setMem(new Mem(0, 0, 0, false, 0, 0, ""));
        }
        if (arg.isConnections()) {
            replyBuilder.setConnections(new Connections(0, 0, 0));
        }
        if (arg.isExtraInfo()) {
            replyBuilder.setExtraInfo(new ExtraInfo("", 0, 0));
        }
        if (arg.isBackgroundFlushing()) {
            replyBuilder.setBackgroundFlushing(new BackgroundFlushing(0, 0, 0, 0, Instant.now()));
        }
        if (arg.isCursors()) {
            replyBuilder.setCursors(new Cursors("", 0, 0, 0, 0, 0));
        }
        if (arg.isNetwork()) {
            replyBuilder.setNetwork(new Network(0, 0, 0));
        }
        //TODO: implement replication status
        //if (arg.isRepl()) {
            //replyBuilder.setRepl(new Repl(setName, ismaster, secondary, primary, hosts, me, electionId, rbid, slaves));
        //}
        if (arg.isOpcountersRepl()) {
            replyBuilder.setOpcountersRepl(new Opcounters(0, 0, 0, 0, 0, 0));
        }
        if (arg.isOpcounters()) {
            replyBuilder.setOpcounters(new Opcounters(0, 0, 0, 0, 0, 0));
        }
        if (arg.isRangeDeleter()) {
            ImmutableList.Builder<RangeDeleter.LastDeletedStat> builder = ImmutableList.builder();
            replyBuilder.setRangeDeleter(new RangeDeleter(builder.build()));
        }
        if (arg.isSecurity()) {
            replyBuilder.setSecurity(new Security(null, false, null));
        }
        if (arg.isStorageEngine()) {
            replyBuilder.setStorageEngine(new StorageEngine("ToroDB"));
        }
        if (arg.isAsserts()) {
            replyBuilder.setAsserts(new Asserts(0, 0, 0, 0, 0));
        }
        if (arg.isDur()) {
            replyBuilder.setDur(new Dur(0, 0, 0, 0, 0, 0, new Dur.TimeMS(0, 0, 0, 0, 0, 0, 0)));
        }
        if (arg.isWriteBacksQueued()) {
            replyBuilder.setWritebacksQueued(0);
        }
        if (arg.isMetrics()) {
            ImmutableList.Builder<Metrics.Command> builder = ImmutableList.builder();
            Metrics.Stats dummyStats = new Metrics.Stats(0, 0);
            replyBuilder.setMetrics(new Metrics(
                    builder.build(), 
                    new Metrics.Document(0, 0, 0, 0), 
                    new Metrics.GetLastError(dummyStats, 0), 
                    new Metrics.Operation(0, 0, 0), 
                    new Metrics.QueryExecutor(0), 
                    new Metrics.Record(0), 
                    new Metrics.Repl(
                            new Metrics.Repl.Apply(dummyStats, 0), 
                            new Metrics.Repl.Buffer(0, 0, 0), 
                            new Metrics.Repl.Network(0, dummyStats, 0, 0), 
                            new Metrics.Repl.Oplog(dummyStats, 0), 
                            new Metrics.Repl.Preload(dummyStats, dummyStats)), 
                    new Metrics.Storage(new Metrics.Storage.Freelist(new Metrics.Storage.Freelist.Search(0, 0, 0))),
                    new Metrics.Ttl(0, 0)));
        }
        
        return Status.ok(replyBuilder.build());
    }

}
