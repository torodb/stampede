
package com.torodb.mongodb.commands.impl;

import com.torodb.mongodb.core.MongodTransaction;

/**
 *
 */
public interface ReadTorodbCommandImpl<Arg, Result> extends TorodbCommandImpl<Arg, Result, MongodTransaction> {

    @Override
    default public boolean requiresWritePermission() {
        return false;
    }

}
