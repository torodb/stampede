
package com.torodb.mongodb.commands.impl.diagnostic;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.mongodb.commands.impl.AbstractTorodbCommandImplementation;
import com.torodb.mongodb.core.MongodConnection;

/**
 *
 */
public class PingImplementation extends AbstractTorodbCommandImplementation<Empty, Empty>{

    @Override
    public Status<Empty> apply(Command<? super Empty, ? super Empty> command, Request<Empty> req, MongodConnection connection) {
        return Status.ok(Empty.getInstance());
    }

}
