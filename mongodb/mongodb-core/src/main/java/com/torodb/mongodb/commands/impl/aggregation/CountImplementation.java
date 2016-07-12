
package com.torodb.mongodb.commands.impl.aggregation;

import javax.inject.Singleton;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.aggregation.CountCommand.CountArgument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.core.MongodTransaction;

/**
 *
 */
@Singleton
public class CountImplementation implements ReadTorodbCommandImpl<CountArgument, Long> {

    @Override
    public Status<Long> apply(Request req, Command<? super CountArgument, ? super Long> command, CountArgument arg, MongodTransaction context) {
            return Status.ok(context.getTorodTransaction().countAll(req.getDatabase(), arg.getCollection()));
    }

}
