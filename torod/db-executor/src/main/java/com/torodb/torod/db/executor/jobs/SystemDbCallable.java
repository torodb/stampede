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

package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import java.util.concurrent.Callable;

/**
 *
 */
abstract class SystemDbCallable<R> implements Callable<R> {

    private final DbWrapper dbWrapperPool;

    public SystemDbCallable(DbWrapper dbWrapperPool) {
        this.dbWrapperPool = dbWrapperPool;
    }

    abstract R call(DbConnection db) throws ImplementationDbException, UserDbException;
    
    abstract void doCallback(R result);

    @Override
    public R call() throws ImplementationDbException, UserDbException {
        DbConnection db = dbWrapperPool.getSystemDbConnection();
        try {
            R result = call(db);
            
            db.commit();
            
            doCallback(result);
            
            return result;
        } catch (RuntimeException ex) {
            db.rollback();
            throw ex;
        } catch (Error error) {
            db.rollback();
            throw error;
        } finally {
            db.close();
        }
    }

}
