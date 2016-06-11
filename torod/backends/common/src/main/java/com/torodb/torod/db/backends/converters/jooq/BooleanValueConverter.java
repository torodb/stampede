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
import com.torodb.torod.core.subdocument.values.ScalarBoolean;

/**
 *
 */
public class BooleanValueConverter implements SubdocValueConverter<Boolean, ScalarBoolean> {
    private static final long serialVersionUID = 1L;

    public static final DataTypeForScalar<ScalarBoolean> TYPE = DataTypeForScalar.from(SQLDataType.BOOLEAN, new BooleanValueConverter());

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.BOOLEAN;
    }

    @Override
    public ScalarBoolean from(Boolean databaseObject) {
        return ScalarBoolean.from(databaseObject);
    }

    @Override
    public Boolean to(ScalarBoolean userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<Boolean> fromType() {
        return Boolean.class;
    }

    @Override
    public Class<ScalarBoolean> toType() {
        return ScalarBoolean.class;
    }

}
