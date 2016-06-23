
package com.torodb.mongodb.commands.impl.diagnostic;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.GetLogCommand.AsteriskGetLogReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.GetLogCommand.GetLogArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.GetLogCommand.GetLogReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.GetLogCommand.LogGetLogReply;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.commands.impl.ConnectionTorodbCommandImpl;
import com.torodb.mongodb.core.MongodConnection;
import java.util.Collections;

/**
 *
 */
public class GetLogImplementation extends ConnectionTorodbCommandImpl<GetLogArgument, GetLogReply>{

    @Override
    public Status<GetLogReply> apply(Request req, Command<? super GetLogArgument, ? super GetLogReply> command, GetLogArgument arg, MongodConnection context) {
        if (arg.isIsAsterisk()) {
            return Status.ok(new AsteriskGetLogReply(Collections.emptyList()));
        }
        else {
            switch (arg.getLogName()) {
                case "startupWarnings": { //MOCKED
                    return Status.ok(new LogGetLogReply(0, Collections.emptyList()));
                }
                default: {
                    return Status.from(ErrorCode.COMMAND_FAILED, "no RamLog named " + arg.getLogName());
                }
            }
        }
    }


}
