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
import com.torodb.torod.core.subdocument.values.ScalarInstant;
import com.torodb.torod.core.subdocument.values.heap.InstantScalarInstant;
import java.sql.Timestamp;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;
import org.threeten.bp.DateTimeUtils;

/**
 *
 */
public class InstantValueConverter implements SubdocValueConverter<Timestamp, ScalarInstant>{
    private static final long serialVersionUID = 1L;

    @Override
    public DataType<Timestamp> getDataType() {
        return SQLDataType.TIMESTAMP;
    }

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.DATE;
    }

    @Override
    public ScalarInstant from(Timestamp databaseObject) {
        return new InstantScalarInstant(
                DateTimeUtils.toInstant(databaseObject)
        );
    }

    @Override
    public Timestamp to(ScalarInstant userObject) {
        return DateTimeUtils.toSqlTimestamp(userObject.getValue());
    }

    @Override
    public Class<Timestamp> fromType() {
        return Timestamp.class;
    }

    @Override
    public Class<ScalarInstant> toType() {
        return ScalarInstant.class;
    }
    
}
