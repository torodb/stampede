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

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;

/**
 *
 */
public final class ApplierContext {

    private final boolean updatesAsUpserts;

    public ApplierContext(boolean updatesAsUpserts) {
        this.updatesAsUpserts = updatesAsUpserts;
    }

    /**
     * Returns true if this updates must be executed as upserts.
     *
     * When this is true, {@link UpdateOplogOperation update oplog operations} should never fail.
     * @return
     */
    public boolean treatUpdateAsUpsert() {
        return updatesAsUpserts;
    }
}
