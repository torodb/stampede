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
import java.util.Arrays;

import javax.inject.Singleton;

import org.jooq.exception.DataAccessException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.torodb.backend.exceptions.BackendException;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;

/**
 *
 */
@Singleton
public abstract class AbstractErrorHandler implements ErrorHandler {

    private final ImmutableMap<String, ImmutableSet<Context>> rollbackRules;
    
    protected AbstractErrorHandler(RollbackRule...rollbackRules) {
        ImmutableMap.Builder<String, ImmutableSet<Context>> rollbackRulesBuilder =
                ImmutableMap.builder();
        
        for (RollbackRule rollbackRule : rollbackRules) {
            rollbackRulesBuilder.put(rollbackRule.sqlCode, Sets.immutableEnumSet(Arrays.asList(rollbackRule.contexts)));
        }
        
        this.rollbackRules = rollbackRulesBuilder.build();
    }
    
    @Override
    public ToroRuntimeException handleException(Context context, SQLException sqlException) throws RollbackException {
        try {
            return handleUserException(context, sqlException);
        } catch(UserException userException) {
            return new BackendException(context, sqlException);
        }
    }

    @Override
    public ToroRuntimeException handleException(Context context, DataAccessException dataAccessException) throws RollbackException {
        try {
            return handleUserException(context, dataAccessException);
        } catch(UserException userException) {
            return new BackendException(context, dataAccessException);
        }
    }

    @Override
    public ToroRuntimeException handleUserException(Context context, SQLException sqlException) throws UserException, RollbackException {
        if (applyToRollbackRule(context, sqlException.getSQLState())) {
            throw new RollbackException(sqlException);
        }
        
        return new BackendException(context, sqlException);
    }

    @Override
    public ToroRuntimeException handleUserException(Context context, DataAccessException dataAccessException) throws UserException, RollbackException {
        if (applyToRollbackRule(context, dataAccessException.sqlState())) {
            throw new RollbackException(dataAccessException);
        }
        
        return new BackendException(context, dataAccessException);
    }

    private boolean applyToRollbackRule(Context context, String sqlState) {
        ImmutableSet<Context> contexts = rollbackRules.get(sqlState);
        if (contexts != null && (contexts.isEmpty() || contexts.contains(context))) {
            return true;
        }
        
        return false;
    }
    
    protected static RollbackRule rule(String sqlCode, Context...contexts) {
        return new RollbackRule(sqlCode, contexts);
    }
    
    protected static class RollbackRule {
        private String sqlCode;
        private Context[] contexts;
        
        private RollbackRule(String code, Context[] contexts) {
            this.sqlCode = code;
            this.contexts = contexts;
        }
    }
}
