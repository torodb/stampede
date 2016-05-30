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


package com.torodb.poc.backend.converters.jooq;

import org.jooq.impl.SQLDataType;

import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVInteger;


/**
 *
 */
public class IntegerValueConverter implements KVValueConverter<Integer, KVInteger>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVInteger> TYPE = DataTypeForKV.from(SQLDataType.INTEGER, new IntegerValueConverter());

    @Override
    public KVType getErasuredType() {
        return IntegerType.INSTANCE;
    }

    @Override
    public KVInteger from(Integer databaseObject) {
        return KVInteger.of(databaseObject);
    }

    @Override
    public Integer to(KVInteger userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<Integer> fromType() {
        return Integer.class;
    }

    @Override
    public Class<KVInteger> toType() {
        return KVInteger.class;
    }
    
}
