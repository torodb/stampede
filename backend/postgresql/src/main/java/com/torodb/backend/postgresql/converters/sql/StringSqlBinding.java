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

package com.torodb.backend.postgresql.converters.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.postgresql.converters.util.SqlEscaper;
import com.torodb.common.util.TextEscaper;

public class StringSqlBinding implements SqlBinding<String> {
    public static final StringSqlBinding INSTANCE = new StringSqlBinding();
    
    private static final TextEscaper ESCAPER = SqlEscaper.INSTANCE;

    @Override
    public String get(ResultSet resultSet, int columnIndex) throws SQLException {
        String value = resultSet.getString(columnIndex);
        if (resultSet.wasNull()) {
            return null;
        }
        return ESCAPER.unescape(value);
    }

    @Override
    public void set(PreparedStatement preparedStatement, int parameterIndex, String value) throws SQLException {
        preparedStatement.setString(parameterIndex, ESCAPER.escape(value));
    }
}
