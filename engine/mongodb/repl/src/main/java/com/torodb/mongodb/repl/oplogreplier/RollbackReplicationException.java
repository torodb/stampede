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

package com.torodb.mongodb.repl.oplogreplier;

/**
 * The exception that is thrown by replication modules when they think a replication rollback must
 * be done because a recoverable problem.
 *
 * It is important to know that a rollback on replication has nothing to do with a rollback on SQL
 * databases.
 */
public class RollbackReplicationException extends Exception {

  private static final long serialVersionUID = -2363246626369264374L;

  public RollbackReplicationException(String message) {
    super(message);
  }

  public RollbackReplicationException(String message, Throwable cause) {
    super(message, cause);
  }

  public RollbackReplicationException(Throwable cause) {
    super(cause);
  }

}
