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

package com.torodb.backend.converters.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface SqlBinding<T> {
    /**
     * Return the value from the {@code ResultSet} at the specified {@code columnIndex}.
     * If the value in the {@code ResultSet} is null it must return null too.
     * @param resultSet
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    public @Nullable T get(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException;
    
    /**
     * Set the parameter of {@code PreparedStatement} at specified {@code parameterIndex}.
     * @param preparedStatement
     * @param parameterIndex
     * @param value
     * @throws SQLException
     */
    public void set(@Nonnull PreparedStatement preparedStatement, int parameterIndex, @Nonnull T value) throws SQLException;
    
    /**
     * Return the placeholder for a value to use in SQL statement.
     * @return
     */
    default public String getPlaceholder() {
        return "?";
    }
}
