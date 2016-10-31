/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl;

/**
 *
 */
public enum ReplCoordinatorState {

    /**
     * The replication coordinator is not started yet.
     */
    STARTUP,
    /**
     * Replication coordinator is on recovery mode.
     */
    RECOVERY,
    /**
     * Replication coordinator is on secondary mode.
     */
    SECONDARY,
    /**
     * Replication coordinator is not an active mode (it is initating or
     * transiting to another state).
     */
    IDLE,
    ERROR,
    TERMINATED;

}
