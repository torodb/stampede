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
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UserToroException;

/**
 *
 */
abstract class SystemDbCallable<R> extends Job<R> {

    private final DbWrapper dbWrapperPool;
    private DbConnection connection = null;

    public SystemDbCallable(DbWrapper dbWrapperPool) {
        this.dbWrapperPool = dbWrapperPool;
    }

    abstract R call(DbConnection db) throws ImplementationDbException,
            UserDbException;

    abstract void doCallback(R result);

    @Override
    protected R onFail(Throwable t) throws ToroException, ToroRuntimeException {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
            catch (UserDbException ex) {
                throw new UserToroException(ex);
            }
        }
        if (t instanceof ToroException) {
            throw (ToroException) t;
        }
        throw new ToroRuntimeException(t);
    }

    @Override
    protected R failableCall() throws ToroException, ToroRuntimeException {
        try {
            connection = dbWrapperPool.getSystemDbConnection();
            try {
                R result = call(connection);

                connection.commit();

                doCallback(result);

                return result;
            }
            finally {
                connection.close();
            }
        } catch (ImplementationDbException ex) {
            throw new ToroImplementationException(ex);
        } catch (UserDbException ex) {
            throw new UserToroException(ex);
        }
    }

}
