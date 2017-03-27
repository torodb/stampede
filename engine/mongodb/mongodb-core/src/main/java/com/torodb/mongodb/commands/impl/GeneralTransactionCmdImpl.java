/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.CmdImplMapSupplier;
import com.torodb.mongodb.commands.impl.admin.ListCollectionsImplementation;
import com.torodb.mongodb.commands.impl.admin.ListIndexesImplementation;
import com.torodb.mongodb.commands.impl.aggregation.CountImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.CollStatsImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.ListDatabasesImplementation;
import com.torodb.mongodb.commands.impl.general.FindImplementation;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand;
import com.torodb.mongodb.commands.signatures.aggregation.CountCommand;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand;
import com.torodb.mongodb.commands.signatures.diagnostic.ListDatabasesCommand;
import com.torodb.mongodb.commands.signatures.general.FindCommand;
import com.torodb.mongodb.core.MongodTransaction;

import java.util.Set;

import javax.inject.Inject;

/**
 * This class contains the implementations of all commands that can be executed on a read or write
 * transaction.
 */
@SuppressWarnings("checkstyle:LineLength")
public class GeneralTransactionCmdImpl implements CmdImplMapSupplier<MongodTransaction> {

  private final ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodTransaction>> map;

  @Inject
  GeneralTransactionCmdImpl(LoggerFactory loggerFactory) {
    map = ImmutableMap.<Command<?, ?>, CommandImplementation<?, ?, ? super MongodTransaction>>builder()
        .put(ListCollectionsCommand.INSTANCE, new ListCollectionsImplementation(loggerFactory))
        .put(ListIndexesCommand.INSTANCE, new ListIndexesImplementation())
        .put(CountCommand.INSTANCE, new CountImplementation())
        .put(CollStatsCommand.INSTANCE, new CollStatsImplementation(loggerFactory))
        .put(ListDatabasesCommand.INSTANCE, new ListDatabasesImplementation())
        .put(FindCommand.INSTANCE, new FindImplementation(loggerFactory))
        .build();
  }

  Set<Command<?, ?>> getSupportedCommands() {
    return map.keySet();
  }

  @Override
  public ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodTransaction>> get() {
    return map;
  }

}
