
package com.torodb.mongodb.commands.impl;

import com.torodb.mongodb.core.MongodTransaction;

/**
 *
 */
public abstract class ReadTorodbCommandImpl<Arg, Result> extends TorodbCommandImpl<Arg, Result, MongodTransaction> {

    @Override
    public boolean requiresWritePermission() {
        return false;
    }

}
