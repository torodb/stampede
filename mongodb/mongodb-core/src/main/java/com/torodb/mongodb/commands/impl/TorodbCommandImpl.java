
package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.torodb.mongodb.core.MongodTransaction;

/**
 *
 */
public abstract class TorodbCommandImpl<Arg, Result, MT extends MongodTransaction> implements CommandImplementation<Arg, Result, MT>{

    public abstract boolean requiresWritePermission();

}
