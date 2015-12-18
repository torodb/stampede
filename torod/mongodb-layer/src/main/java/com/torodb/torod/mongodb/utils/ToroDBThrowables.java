
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.UserToroException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.currentThread;

/**
 *
 */
public class ToroDBThrowables {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(ToroDBThrowables.class);
    private ToroDBThrowables() {}

    public static <E> E getFromCommand(String commandName, Future<E> future) 
            throws CommandFailed, InternalError {
        try {
            return future.get();
        } catch (InterruptedException ex) {
            currentThread().interrupt();
            throw new CommandFailed(commandName, "Interrupted while executing");
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause != null && (cause instanceof ToroException || cause instanceof UserToroException)) {
                throw new CommandFailed(commandName, cause.getLocalizedMessage());
            }
            LOGGER.warn("Internal error while executing " + commandName, ex);
            throw new InternalError();
        }
    }

}
