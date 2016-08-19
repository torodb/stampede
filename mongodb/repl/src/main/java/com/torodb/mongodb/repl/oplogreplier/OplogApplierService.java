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

import com.google.common.util.concurrent.Service;

/**
 *
 */
public interface OplogApplierService extends Service {

    public static interface Callback {
        void rollback(RollbackReplicationException ex);

        void onFinish();

        void onError(Throwable t);
    }

    public static interface OplogApplierServiceFactory {
        OplogApplierService createOplogApplier(Callback callback);
    }

}