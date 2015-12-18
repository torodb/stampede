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

import com.google.common.collect.Lists;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.WriteError;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.DbException;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class DeleteCallable extends TransactionalJob<DeleteResponse> {

    private final Report report;
    private final String collection;
    private final List<? extends DeleteOperation> deletes;
    private final WriteFailMode mode;

    public DeleteCallable(
            DbConnection connection,
            TransactionAborter abortCallback,
            Report report, 
            String collection, 
            List<? extends DeleteOperation> deletes, 
            WriteFailMode mode) {
        super(connection, abortCallback);
        this.report = report;
        this.collection = collection;
        this.deletes = deletes;
        this.mode = mode;
    }

    @Override
    protected DeleteResponse failableCall() throws ToroException, ToroRuntimeException {
        try {
            DeleteResponse response;
            switch (mode) {
                case ISOLATED:
                    response = isolatedDelete();
                    break;
                case ORDERED:
                    response = orderedDelete();
                    break;
                case TRANSACTIONAL:
                    response = transactionalDelete();
                    break;
                default:
                    throw new AssertionError("Study exceptions");
            }
            report.deleteExecuted(collection, deletes, mode);
            return response;
        }
        catch (ImplementationDbException ex) {
            throw new ToroImplementationException(ex);
        }
        catch (UserDbException ex) {
            throw new UserToroException(ex);
        }
    }

    private DeleteResponse isolatedDelete() throws ImplementationDbException {
        DbConnection connection = getConnection();
        int deleted = 0;
        List<WriteError> errors = Lists.newLinkedList();

        for (int index = 0; index < deletes.size(); index++) {
            DeleteOperation delete = deletes.get(index);
            
            try {
                deleted += connection.delete(collection, delete.getQuery(), delete.isJustOne());
            } catch (UserDbException ex) {
                appendError(errors, ex, index);
            }
        }
        
        return createResponse(deleted, errors);
    }

    private DeleteResponse orderedDelete() throws ImplementationDbException, UserDbException {
        DbConnection connection = getConnection();
        int deleted = 0;
        List<WriteError> errors = Lists.newLinkedList();
        int index = -1;
        
        try {
            for (index = 0; index < deletes.size(); index++) {
                DeleteOperation delete = deletes.get(index);
                
                deleted += connection.delete(collection, delete.getQuery(), delete.isJustOne());
            }
        } catch (UserDbException ex) {
            assert index != -1;
            appendError(errors, ex, index);
        }
        
        return createResponse(deleted, errors);
    }

    private DeleteResponse transactionalDelete() throws ImplementationDbException, UserDbException {
        DbConnection connection = getConnection();
        int deleted = 0;
        List<WriteError> errors = Lists.newLinkedList();
        int index = -1;
        
        try {
            for (index = 0; index < deletes.size(); index++) {
                DeleteOperation delete = deletes.get(index);
                
                deleted += connection.delete(collection, delete.getQuery(), delete.isJustOne());
            }
        } catch (UserDbException ex) {
            assert index >= 0;
            appendError(errors, ex, index);
            connection.rollback();
        }
        
        return createResponse(deleted, errors);
    }

    private int getErrorCode(DbException ex) {
        return -1;
    }

    private String getErrorMessage(DbException ex) {
        return ex.getMessage();
    }
    
    private void appendError(@Nonnull List<WriteError> errors, DbException ex, int index) {
        errors.add(new WriteError(index, getErrorCode(ex), getErrorMessage(ex)));
    }
    
    private DeleteResponse createResponse(int deleted, @Nullable List<WriteError> errors) {
        if (errors == null || errors.isEmpty()) {
            return new DeleteResponse(true, deleted, null);
        }
        return new DeleteResponse(false, deleted, errors);
    }
    
    public static interface Report {
        public void deleteExecuted(
            String collection, 
            List<? extends DeleteOperation> deletes, 
            WriteFailMode mode);
    }
}
