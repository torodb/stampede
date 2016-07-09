package com.torodb.mongodb.commands.impl;

import com.torodb.mongodb.core.WriteMongodTransaction;

/**
 *
 */
public interface WriteTorodbCommandImpl<Arg, Result> extends TorodbCommandImpl<Arg, Result, WriteMongodTransaction>{

    @Override
    default public boolean requiresWritePermission() {
        return true;
    }

}
