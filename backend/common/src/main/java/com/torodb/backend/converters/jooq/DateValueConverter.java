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

package com.torodb.backend.converters.jooq;

import java.sql.Date;
import java.time.LocalDate;

import org.jooq.impl.SQLDataType;

import com.torodb.kvdocument.types.DateType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVDate;
import com.torodb.kvdocument.values.heap.LocalDateKVDate;

/**
 *
 */
public class DateValueConverter implements KVValueConverter<Date, KVDate> {
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVDate> TYPE = DataTypeForKV.from(SQLDataType.DATE, new DateValueConverter());

    @Override
    public KVType getErasuredType() {
        return DateType.INSTANCE;
    }

    @Override
    public KVDate from(Date databaseObject) {
        return new LocalDateKVDate(
                LocalDate.of(databaseObject.getYear() + 1900, databaseObject.getMonth() + 1, databaseObject.getDate())
        );
    }

    @Override
    public Date to(KVDate userObject) {
        LocalDate date = userObject.getValue();
        return new Date(date.getYear() - 1900, date.getMonthValue() -1, date.getDayOfMonth());
    }

    @Override
    public Class<Date> fromType() {
        return Date.class;
    }

    @Override
    public Class<KVDate> toType() {
        return KVDate.class;
    }

}