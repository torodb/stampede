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

package com.torodb.mongodb.repl.impl;

import com.google.common.util.concurrent.AbstractService;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.mongodb.repl.ConsistencyHandler;

/**
 * A {@link ConsistencyHandler} that behaves as the handled resource is always consistent.
 */
public class AlwaysConsistentConsistencyHandler extends AbstractService
    implements ConsistencyHandler {

  @Override
  public boolean isConsistent() {
    return true;
  }

  @Override
  public void setConsistent(boolean newConsistency) throws RetrierGiveUpException {
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doStop() {
  }

}
