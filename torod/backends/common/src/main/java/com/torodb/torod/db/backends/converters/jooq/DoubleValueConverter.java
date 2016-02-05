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

package com.torodb.torod.db.backends.converters.jooq;

import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarDouble;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class DoubleValueConverter implements SubdocValueConverter<Double, ScalarDouble> {
    private static final long serialVersionUID = 1L;

    @Override
    public DataType<Double> getDataType() {
        return SQLDataType.DOUBLE;
    }

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.DOUBLE;
    }

    @Override
    public ScalarDouble from(Double databaseObject) {
        return ScalarDouble.of(databaseObject);
    }

    @Override
    public Double to(ScalarDouble userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<Double> fromType() {
        return Double.class;
    }

    @Override
    public Class<ScalarDouble> toType() {
        return ScalarDouble.class;
    }

}
