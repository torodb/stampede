
package com.torodb.mongodb.commands.impl.replication;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.IsMasterCommand.IsMasterReply;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.impl.AbstractTorodbCommandImplementation;
import com.torodb.mongodb.core.MongoLayerConstants;
import com.torodb.mongodb.core.MongodConnection;
import java.time.Clock;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class IsMasterCommandImplementation extends AbstractTorodbCommandImplementation<Empty, IsMasterReply> {

    private final Clock clock;
    private final HostAndPort me;

    @Inject
    public IsMasterCommandImplementation(Clock clock, HostAndPort me) {
        this.clock = clock;
        this.me = me;
    }

    @Override
    public Status<IsMasterReply> apply(Command<? super Empty, ? super IsMasterReply> command, Request<Empty> req, MongodConnection connection) {

        return Status.ok(
                new IsMasterReply(
                        true,
                        "torodb-rs",
                        0,
                        null,
                        null,
                        null,
                        me,
                        false,
                        false,
                        true,
                        true,
                        0,
                        null,
                        me,
                        null,
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
