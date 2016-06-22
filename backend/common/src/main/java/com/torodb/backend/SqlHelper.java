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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.exceptions.SystemException;

@Singleton
public class SqlHelper {

    private final DataTypeProvider dataTypeProvider;
    private final ErrorHandler errorHandler;
    
    @Inject
    public SqlHelper(DataTypeProvider dataTypeProvider, ErrorHandler errorHandler) {
        super();
        this.dataTypeProvider = dataTypeProvider;
        this.errorHandler = errorHandler;
    }

    public void executeStatement(DSLContext dsl, String statement, Context context){
        Connection c = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement ps = c.prepareStatement(statement)) {
            ps.execute();
        } catch (SQLException ex) {
            errorHandler.handleRollbackException(context, ex);
            throw new SystemException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }       
    }

    public Result<Record> executeStatementWithResult(DSLContext dsl, String statement, Context context){
        Connection c = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement ps = c.prepareStatement(statement)) {
            ResultSet resultSet = ps.executeQuery();
            return dsl.fetch(resultSet);
        } catch (SQLException ex) {
            errorHandler.handleRollbackException(context, ex);
            throw new SystemException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }
    }
    
    public void executeUpdate(DSLContext dsl, String statement, Context context){
        Connection c = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement ps = c.prepareStatement(statement)) {
            ps.execute();
        } catch (SQLException ex) {
            errorHandler.handleRollbackException(context, ex);
            throw new SystemException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }       
    }
    
    public void executeUpdate(Connection c, String statement, Context context){
        try (PreparedStatement ps = c.prepareStatement(statement)) {
            ps.execute();
        } catch (SQLException ex) {
            errorHandler.handleRollbackException(context, ex);
            throw new SystemException(ex);
        }       
    }

    public String renderVal(String value) {
        return DSL.using(dataTypeProvider.getDialect()).render(DSL.val(value));
    }
}
