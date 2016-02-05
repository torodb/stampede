package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.google.common.collect.ImmutableMap;
import com.torodb.torod.mongodb.commands.impl.torodb.CreatePathViewsImplementation;
import com.torodb.torod.mongodb.commands.impl.torodb.DropPathViewsImplementation;
import com.torodb.torod.mongodb.commands.impl.torodb.SqlSelectImplementation;
import com.torodb.torod.mongodb.commands.torodb.CreatePathViewsCommand;
import com.torodb.torod.mongodb.commands.torodb.DropPathViewsCommand;
import com.torodb.torod.mongodb.commands.torodb.SqlSelectCommand;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class ToroDBSpecificCommandTool {

    private final ImmutableMap<Command<?,?>, CommandImplementation> map;

    public ToroDBSpecificCommandTool() {
        this.map = ImmutableMap.<Command<?,?>, CommandImplementation>builder()
                .put(CreatePathViewsCommand.INSTANCE, new CreatePathViewsImplementation())
                .put(DropPathViewsCommand.INSTANCE, new DropPathViewsImplementation())
                .put(SqlSelectCommand.INSTANCE, new SqlSelectImplementation())
                .build();
    }

    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public ImmutableMap<Command<?,?>, CommandImplementation> getMap() {
        return map;
    }

}
