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

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import com.torodb.backend.interfaces.ErrorHandlerInterface.Context;
import com.torodb.core.backend.DidCursor;
import com.torodb.core.exceptions.SystemException;

public class DefaultDidCursor implements DidCursor {
    public final SqlInterface sqlInterface;
    public final ResultSet resultSet;

    public DefaultDidCursor(@Nonnull SqlInterface sqlInterface, @Nonnull ResultSet resultSet) {
        super();
        this.sqlInterface = sqlInterface;
        this.resultSet = resultSet;
    }

    @Override
    public boolean next() {
        try {
            return resultSet.next();
        } catch(SQLException ex) {
            sqlInterface.handleRollbackException(Context.fetch, ex);
            
            throw new SystemException(ex);
        }
    }

    @Override
    public int get() {
        try {
            return resultSet.getInt(1);
        } catch(SQLException ex) {
            sqlInterface.handleRollbackException(Context.fetch, ex);
            
            throw new SystemException(ex);
        }
    }

    @Override
    public void close() {
        try {
            resultSet.close();
        } catch(SQLException ex) {
            sqlInterface.handleRollbackException(Context.fetch, ex);
            
            throw new SystemException(ex);
        }
    }
}
