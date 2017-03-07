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
import com.torodb.core.annotations.DoNotChange;
import com.torodb.mongodb.commands.CmdImplMapSupplier;
import com.torodb.mongodb.commands.impl.admin.RenameCollectionImplementation;
import com.torodb.mongodb.commands.signatures.admin.RenameCollectionCommand;
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@SuppressWarnings("checkstyle:LineLength")
public class ExclusiveWriteTransactionCmdsImpl implements CmdImplMapSupplier<ExclusiveWriteMongodTransaction> {

  private final Map<Command<?, ?>, CommandImplementation<?, ?, ? super ExclusiveWriteMongodTransaction>> map;

  @Inject
  ExclusiveWriteTransactionCmdsImpl() {
    map = Collections.singletonMap(RenameCollectionCommand.INSTANCE, new RenameCollectionImplementation());
  }

  @DoNotChange
  Set<Command<?, ?>> getSupportedCommands() {
    return map.keySet();
  }

  @Override
  public Map<Command<?, ?>, CommandImplementation<?, ?, ? super ExclusiveWriteMongodTransaction>> get() {
    return map;
  }
}
