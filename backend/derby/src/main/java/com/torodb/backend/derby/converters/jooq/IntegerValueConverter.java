/*
 * MongoWP - ToroDB-poc: Backend Derby
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.derby.converters.jooq;

import org.jooq.DataType;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.IntegerSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVInteger;


/**
 *
 */
public class IntegerValueConverter implements KVValueConverter<Integer, Integer, KVInteger>{
    private static final long serialVersionUID = 1L;

    public static final DataType<Integer> INTEGER_TYPE = new DefaultDataType<Integer>(SQLDialect.DERBY, Integer.class, "INTEGER");
    
    public static final DataTypeForKV<KVInteger> TYPE = DataTypeForKV.from(INTEGER_TYPE, new IntegerValueConverter());

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

    @Override
    public SqlBinding<Integer> getSqlBinding() {
        return IntegerSqlBinding.INSTANCE;
    }
}
