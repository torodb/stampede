
package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.torodb.mongodb.core.MongodTransaction;

/**
 *
 */
public interface TorodbCommandImpl<Arg, Result, MT extends MongodTransaction> extends CommandImplementation<Arg, Result, MT>{

    boolean requiresWritePermission();

}
