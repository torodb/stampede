
package com.torodb.mongodb.commands.impl.diagnostic;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.mongodb.commands.impl.ConnectionTorodbCommandImpl;
import com.torodb.mongodb.core.MongodConnection;

/**
 *
 */
public class PingImplementation extends ConnectionTorodbCommandImpl<Empty, Empty>{

    @Override
    public Status<Empty> apply(Request req, Command<? super Empty, ? super Empty> command, Empty arg, MongodConnection context) {
        return Status.ok(Empty.getInstance());
    }

}
