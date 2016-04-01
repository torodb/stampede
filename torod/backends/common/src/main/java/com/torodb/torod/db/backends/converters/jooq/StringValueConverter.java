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

import org.jooq.impl.SQLDataType;

import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarString;
import com.torodb.torod.core.subdocument.values.heap.StringScalarString;

/**
 *
 */
public class StringValueConverter implements SubdocValueConverter<String, ScalarString>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForScalar<ScalarString> TYPE = DataTypeForScalar.from(SQLDataType.VARCHAR, new StringValueConverter());

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.STRING;
    }

    @Override
    public ScalarString from(String databaseObject) {
        return new StringScalarString(databaseObject);
    }

    @Override
    public String to(ScalarString userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<ScalarString> toType() {
        return ScalarString.class;
    }
    
}
