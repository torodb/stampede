package com.torodb.mongodb.commands.impl;

import com.torodb.mongodb.core.WriteMongodTransaction;

/**
 *
 */
public abstract class WriteTorodbCommandImpl<Arg, Result> extends TorodbCommandImpl<Arg, Result, WriteMongodTransaction>{

    @Override
    public boolean requiresWritePermission() {
        return true;
    }

}
