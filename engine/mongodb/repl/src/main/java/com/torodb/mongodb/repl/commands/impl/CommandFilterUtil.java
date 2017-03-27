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

package com.torodb.mongodb.repl.commands.impl;

import com.eightkdata.mongowp.server.api.Command;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.filters.DatabaseFilter;
import com.torodb.mongodb.filters.FilterResult;
import com.torodb.mongodb.filters.NamespaceFilter;
import com.torodb.mongodb.language.Namespace;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * An utility class used to test if a command must be ignored and log a standard message in that
 * case.
 */
@ThreadSafe
public class CommandFilterUtil {

  private final Logger logger;
  private final NamespaceFilter namespaceFilter;
  private final DatabaseFilter dbFilter;

  @Inject
  public CommandFilterUtil(NamespaceFilter namespaceFilter, DatabaseFilter dbFilter, 
      LoggerFactory lf) {
    this.logger = lf.apply(this.getClass());
    this.namespaceFilter = namespaceFilter;
    this.dbFilter = dbFilter;
  }

  protected boolean testDbFilter(String db, Command<?, ?> command) {
    FilterResult<String> filterResult = dbFilter.apply(db);
    if (!filterResult.isSuccessful()) {
      logger.debug("Ignoring command {} on {}. Reason {}",
          () -> command.getCommandName(),
          () -> db,
          filterResult.getReasonAsSupplier(db)
      );
    }
    return filterResult.isSuccessful();
  }

  protected boolean testNamespaceFilter(Namespace ns, Command<?, ?> command) {
    FilterResult<Namespace> filterResult = namespaceFilter.apply(ns);
    log(filterResult, command, ns.getDatabase(), ns.getCollection());
    return filterResult.isSuccessful();
  }

  protected boolean testNamespaceFilter(String db, String col, Command<?, ?> command) {
    FilterResult<Namespace> filterResult = namespaceFilter.apply(db, col);
    log(filterResult, command, db, col);
    return filterResult.isSuccessful();
  }

  private void log(FilterResult<Namespace> filterResult, Command<?, ?> command, String db,
      String col) {
    if (!filterResult.isSuccessful()) {
      logger.debug("Ignoring command {} on {}.{}. Reason: {}",
          () -> command.getCommandName(),
          () -> db,
          () -> col,
          filterResult.getReasonAsSupplier(new Namespace(db, col))
      );
    }
  }


}
