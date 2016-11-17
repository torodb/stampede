/*
 * ToroDB - ToroDB: MongoDB Core
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.mongodb.commands.signatures.repl;

import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand.ApplyOpsArgument;
import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand.ApplyOpsReply;
import com.torodb.mongodb.commands.signatures.repl.IsMasterCommand.IsMasterReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetFreezeCommand.ReplSetFreezeArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetFreezeCommand.ReplSetFreezeReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetGetStatusCommand.ReplSetGetStatusReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetReconfigCommand.ReplSetReconfigArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetStepDownCommand.ReplSetStepDownArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetSyncFromCommand.ReplSetSyncFromReply;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class ReplCommands implements Iterable<Command> {

    private final ImmutableList<Command> commands = ImmutableList.<Command>of(
            ApplyOpsCommand.INSTANCE,
            IsMasterCommand.INSTANCE,
            ReplSetFreezeCommand.INSTANCE,
            ReplSetGetConfigCommand.INSTANCE,
            ReplSetGetStatusCommand.INSTANCE,
            ReplSetInitiateCommand.INSTANCE,
            ReplSetMaintenanceCommand.INSTANCE,
            ReplSetReconfigCommand.INSTANCE,
            ReplSetStepDownCommand.INSTANCE,
            ReplSetSyncFromCommand.INSTANCE
    );

    @Override
    public Iterator<Command> iterator() {
        return commands.iterator();
    }

    public static abstract class ReplCommandsImplementationsBuilder<Context> implements Iterable<Entry<Command<?,?>, CommandImplementation<?, ?, ? super Context>>> {

        public abstract CommandImplementation<ApplyOpsArgument, ApplyOpsReply, ? super Context> getApplyOpsImplementation();

        public abstract CommandImplementation<Empty, IsMasterReply, ? super Context> getIsMasterImplementation();

        public abstract CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply, ? super Context> getReplSetFreezeImplementation();

        public abstract CommandImplementation<Empty, ReplicaSetConfig, ? super Context> getReplSetGetConfigImplementation();

        public abstract CommandImplementation<Empty, ReplSetGetStatusReply, ? super Context> getReplSetGetStatusImplementation();

        public abstract CommandImplementation<ReplicaSetConfig, Empty, ? super Context> getReplSetInitiateImplementation();

        public abstract CommandImplementation<Boolean, Empty, ? super Context> getReplSetMaintenanceImplementation();

        public abstract CommandImplementation<ReplSetReconfigArgument, Empty, ? super Context> getReplSetReconfigImplementation();

        public abstract CommandImplementation<ReplSetStepDownArgument, Empty, ? super Context> getReplSetStepDownImplementation();

        public abstract CommandImplementation<HostAndPort, ReplSetSyncFromReply, ? super Context> getReplSetSyncFromImplementation();

        private Map<Command<?,?>, CommandImplementation<?, ?, ? super Context>> createMap() {
            return ImmutableMap.<Command<?,?>, CommandImplementation<?, ?, ? super Context>>builder()
                    .put(ApplyOpsCommand.INSTANCE, getApplyOpsImplementation())
                    .put(IsMasterCommand.INSTANCE, getIsMasterImplementation())
                    .put(ReplSetFreezeCommand.INSTANCE, getReplSetFreezeImplementation())
                    .put(ReplSetGetConfigCommand.INSTANCE, getReplSetGetConfigImplementation())
                    .put(ReplSetGetStatusCommand.INSTANCE, getReplSetGetStatusImplementation())
                    .put(ReplSetInitiateCommand.INSTANCE, getReplSetInitiateImplementation())
                    .put(ReplSetMaintenanceCommand.INSTANCE, getReplSetMaintenanceImplementation())
                    .put(ReplSetReconfigCommand.INSTANCE, getReplSetReconfigImplementation())
                    .put(ReplSetStepDownCommand.INSTANCE, getReplSetStepDownImplementation())
                    .put(ReplSetSyncFromCommand.INSTANCE, getReplSetSyncFromImplementation())

                    .build();
        }

        @Override
        public Iterator<Entry<Command<?,?>, CommandImplementation<?, ?, ? super Context>>> iterator() {
            return createMap().entrySet().iterator();
        }

    }
    
}
