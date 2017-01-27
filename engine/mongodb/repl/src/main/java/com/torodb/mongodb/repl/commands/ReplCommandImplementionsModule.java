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

package com.torodb.mongodb.repl.commands;

import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongodb.commands.impl.ConnectionCmdImpl;
import com.torodb.mongodb.commands.impl.ExclusiveWriteTransactionCmdsImpl;
import com.torodb.mongodb.commands.impl.GeneralTransactionCmdImpl;
import com.torodb.mongodb.commands.impl.WriteTransactionCmdImpl;
import com.torodb.mongodb.core.MongodServerConfig;

public class ReplCommandImplementionsModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(MongodServerConfig.class)
        .toInstance(new MongodServerConfig(HostAndPort.fromParts("localhost", 27017)));

    bind(CommandClassifier.class)
        .in(Singleton.class);

    bind(ConnectionCmdImpl.class)
        .in(Singleton.class);

    bind(GeneralTransactionCmdImpl.class)
        .in(Singleton.class);

    bind(WriteTransactionCmdImpl.class)
        .to(ReplWriteTransactionImplementations.class)
        .in(Singleton.class);

    bind(ExclusiveWriteTransactionCmdsImpl.class)
        .to(ReplExclusiveWriteTransactionImplementations.class)
        .in(Singleton.class);
  }

}
