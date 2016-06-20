
package com.torodb.mongodb.commands.impl.authentication;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.mongodb.commands.impl.ConnectionTorodbCommandImpl;
import com.torodb.mongodb.core.MongodConnection;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class GetNonceImplementation extends ConnectionTorodbCommandImpl<Empty, String> {

    private static final Logger LOGGER = LogManager.getLogger(GetNonceImplementation.class);

    @Override
    public Status<String> apply(Request req, Command<? super Empty, ? super String> command, Empty arg, MongodConnection context) {
        LOGGER.warn("Authentication not supported. Operation 'getnonce' called. A fake value is returned");

        Random r = new Random();
        String nonce = Long.toHexString(r.nextLong());

        return Status.ok(nonce);
    }

}
