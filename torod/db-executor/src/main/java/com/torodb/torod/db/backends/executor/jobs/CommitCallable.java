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
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UserToroException;

/**
 *
 */
public class CommitCallable extends TransactionalJob<Void> {
    
    private final Report report;

    public CommitCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report) {
        super(connection, abortCallback);
        this.report = report;
    }


    @Override
    protected Void failableCall() throws ToroException, ToroRuntimeException {
        try {
            getConnection().commit();
            
            report.commitExecuted();
            
            return null;
        }
        catch (ImplementationDbException ex) {
            throw new ToroImplementationException(ex);
        }
        catch (UserDbException ex) {
            throw new UserToroException(ex);
        }
    }
      
    public static interface Report {
        public void commitExecuted();
    }
}
