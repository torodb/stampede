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


package com.torodb.torod.db.backends.mysql;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.base.Preconditions;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.db.backends.ArraySerializer;

/**
 *
 */
@Singleton
public class MySQLJsonArraySerializer implements ArraySerializer {

    private static final long serialVersionUID = 946395628;

    @Override
    public String getFieldName(String[] keys) {
        return getFieldName(keys, 0, keys.length);
    }

    @Override
    public String getFieldName(String[] keys, int from, int to) {
        Preconditions.checkArgument(to > from, "'to' must be greater than 'from'");

        if (from + 1 == to) {
            StringBuilder sb = new StringBuilder(4);
            sb.append('`');
            sb.append(keys[from]);
            sb.append('`');
            
            return sb.toString();
        } else {
            StringBuilder sb = new StringBuilder(19 + (to - from + 1) * 2);
            
            sb.append("json_extract(`");
            sb.append(keys[from]);
            sb.append("`,'$");
            
            for (int i = from + 1; i < to; i++) {
                sb.append('.').append(keys[i]);
            }
            
            sb.append("')");
            
            return sb.toString();
        }
    }

    @Nonnull
    @Override
    public Condition typeof(String fieldName, String typeName) {
        return DSL.condition("json_type(?) = '?'", fieldName, typeName);
    }

    @Nonnull
    @Override
    public Condition typeof(String fieldName, String typeName, Condition condition) {
        return typeof(fieldName, typeName).and(condition);
    }

    @Nonnull
    @Override
    public Condition arrayLength(String fieldName, Param<?> value) {
        return DSL.condition("json_length(?) = '?'", fieldName, value);
    }

    @Nonnull
    @Override
    public Table arrayElements(Field<?> iteratorVariable, Field<?> field) {
        //TODO: We need a workaround to search in arrays
        // * MySQL does not support functions that returns set of values
        // * MySQL does not support using column in subqueries of subqueries in where clauses ()

        //StringBuilder mysqlJsonArrayExtractor = new StringBuilder();
        //mysqlJsonArrayExtractor.append("SELECT @next:=@current+1, json_extract(");
        //mysqlJsonArrayExtractor.append(fieldName);
        //mysqlJsonArrayExtractor.append(", '$[@current]') AS value, @current:=@next");
        //mysqlJsonArrayExtractor.append(" FROM (SELECT @current := 0, @length := json_length(");
        //mysqlJsonArrayExtractor.append(fieldName);
        //mysqlJsonArrayExtractor.append(")) init JOIN (SELECT * FROM cartesian");
        //mysqlJsonArrayExtractor.append(" WHERE n0 <= @length&255 AND n1 <= @length>>8&255 AND n2 <= @length>>16&255) array_elements");
        //return DSL.table(mysqlJsonArrayExtractor.toString());
        //return DSL.table("(SELECT NULL AS " + iteratorVariableName + ") AS unsupported");
        
        throw new UserToroException("query on arrays are not supported for MySQL");
    }

}
