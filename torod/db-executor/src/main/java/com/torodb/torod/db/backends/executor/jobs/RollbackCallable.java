/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RollbackCallable extends TransactionalJob<Void> {
    
    private final Report report;
    private final static Logger LOGGER = LoggerFactory.getLogger(TransactionalJob.class);

    public RollbackCallable(
            DbConnection connection, 
            TransactionAborter aborter,
            Report report) {
        super(connection, aborter);
        this.report = report;
    }

    @Override
    protected Void failableCall() throws ToroException, ToroRuntimeException {
        try {
            getConnection().rollback();
            report.rollbackExecuted();
            return null;
        }
        catch (ImplementationDbException ex) {
            LOGGER.error("Error while rollbacking a transaction!");
            throw new ToroImplementationException(ex);
        }
    }
    
    public static interface Report {
        public void rollbackExecuted();
    }
}
