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

package com.torodb.backend;

import java.sql.SQLException;

import javax.inject.Singleton;

import org.jooq.exception.DataAccessException;

import com.google.common.collect.ImmutableSet;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;

/**
 *
 */
@Singleton
public abstract class AbstractErrorHandlerInterface implements ErrorHandler {

    private final ImmutableSet<String> rollbackSqlErrorCodes;
    
    protected AbstractErrorHandlerInterface(String ... rollbackErrorCodes) {
        ImmutableSet.Builder<String> rollbackSqlErrorCodesBuilder =
                ImmutableSet.builder();
        
        for (String rollbackErrorCode : rollbackErrorCodes) {
            rollbackSqlErrorCodesBuilder.add(rollbackErrorCode);
        }
        
        rollbackSqlErrorCodes = rollbackSqlErrorCodesBuilder.build();
    }
    
    @Override
    public void handleRollbackException(Context context, SQLException sqlException) {
        if (rollbackSqlErrorCodes.contains(sqlException.getSQLState())) {
            throw new RollbackException(sqlException);
        }
    }

    @Override
    public void handleRollbackException(Context context, DataAccessException dataAccessException) {
        if (rollbackSqlErrorCodes.contains(dataAccessException.sqlState())) {
            throw new RollbackException(dataAccessException);
        }
    }

    @Override
    public void handleUserAndRetryException(Context context, SQLException sqlException) throws UserException {
        handleRollbackException(context, sqlException);
    }

    @Override
    public void handleUserAndRetryException(Context context, DataAccessException dataAccessException) throws UserException {
        handleRollbackException(context, dataAccessException);
    }
}
