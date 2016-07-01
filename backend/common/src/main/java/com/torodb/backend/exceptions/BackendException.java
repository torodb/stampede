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


package com.torodb.backend.exceptions;

import java.sql.SQLException;

import org.jooq.exception.DataAccessException;

import com.torodb.backend.ErrorHandler;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.exceptions.SystemException;

/**
 * 
 */
public class BackendException extends SystemException {
    
    private static final long serialVersionUID = 1L;
    
    private final Context context;
    private final String sqlState;
    
    private static String sqlMessage(SQLException cause) {
        while(cause.getNextException() != null) {
            cause = cause.getNextException();
        }
        
        return cause.getMessage();
    }
    
    private static String sqlMessage(DataAccessException cause) {
        if (cause.getCause() instanceof SQLException) {
            return sqlMessage((SQLException) cause.getCause()); 
        }
        
        return cause.getMessage();
    }

    public BackendException(Context context, SQLException cause) {
        super(sqlMessage(cause), cause);
        
        this.context = context;
        this.sqlState = cause.getSQLState();
    }

    public BackendException(Context context, DataAccessException cause) {
        super(sqlMessage(cause), cause);
        
        this.context = context;
        this.sqlState = cause.sqlState();
    }

    public Context getContext() {
        return context;
    }

    public String getSqlState() {
        return sqlState;
    }
}
