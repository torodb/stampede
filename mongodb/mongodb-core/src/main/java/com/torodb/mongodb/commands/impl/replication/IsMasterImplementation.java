
package com.torodb.mongodb.commands.impl.replication;

import java.time.Clock;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.IsMasterCommand.IsMasterReply;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.mongodb.commands.impl.ConnectionTorodbCommandImpl;
import com.torodb.mongodb.core.MongoLayerConstants;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServerConfig;

/**
 *
 */
@Singleton
public class IsMasterImplementation extends ConnectionTorodbCommandImpl<Empty, IsMasterReply> {

    private final Clock clock;

    @Inject
    public IsMasterImplementation(Clock clock, MongodServerConfig ourConfig) {
        this.clock = clock;
    }

    @Override
    public Status<IsMasterReply> apply(Request req, Command<? super Empty, ? super IsMasterReply> command, Empty arg, MongodConnection context) {
        return Status.ok(
                new IsMasterReply(
                        MongoLayerConstants.MAX_BSON_DOCUMENT_SIZE,
                        MongoLayerConstants.MAX_MESSAGE_SIZE_BYTES,
                        MongoLayerConstants.MAX_WRITE_BATCH_SIZE,
                        clock.instant(),
                        MongoLayerConstants.MAX_WIRE_VERSION,
                        MongoLayerConstants.MIN_WIRE_VERSION
                )
        );

    }
}
