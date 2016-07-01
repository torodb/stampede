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

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.backend.DidCursor;
import com.torodb.core.exceptions.SystemException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

public class DefaultDidCursor implements DidCursor {
    public final SqlInterface sqlInterface;
    public final ResultSet resultSet;
    public boolean movedNext = false;
    public boolean hasNext = false;

    public DefaultDidCursor(@Nonnull SqlInterface sqlInterface, @Nonnull ResultSet resultSet) {
        this.sqlInterface = sqlInterface;
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            if (!movedNext) {
                hasNext = resultSet.next();
                movedNext = true;
            }
            
            return hasNext;
        } catch(SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
        }
    }

    @Override
    public Integer next() {
        try {
            hasNext();
            movedNext = false;
            
            return resultSet.getInt(1);
        } catch(SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
        }
    }

    @Override
    public void close() {
        try {
            resultSet.close();
        } catch(SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
        }
    }

    @Override
    public Collection<Integer> getNextBatch(final int maxSize) {
        List<Integer> dids = new ArrayList<>();
        
        for (int index = 0; index < maxSize && hasNext(); index++) {
            dids.add(next());
        }
        
        return dids;
    }

    @Override
    public Collection<Integer> getRemaining() {
        List<Integer> dids = new ArrayList<>();
        
        while (hasNext()) {
            dids.add(next());
        }
        
        return dids;
    }
}
