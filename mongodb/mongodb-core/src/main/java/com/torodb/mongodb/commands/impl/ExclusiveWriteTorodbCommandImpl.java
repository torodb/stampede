package com.torodb.mongodb.commands.impl;

import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;

/**
 *
 */
public interface ExclusiveWriteTorodbCommandImpl<Arg, Result> extends TorodbCommandImpl<Arg, Result, ExclusiveWriteMongodTransaction>{

    @Override
    default public boolean requiresWritePermission() {
        return true;
    }

}
