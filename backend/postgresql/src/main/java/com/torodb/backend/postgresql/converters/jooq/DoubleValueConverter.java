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

package com.torodb.backend.postgresql.converters.jooq;

import java.sql.Types;

import org.jooq.util.postgres.PostgresDataType;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.DoubleSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVDouble;

/**
 *
 */
public class DoubleValueConverter implements KVValueConverter<Double, Double, KVDouble> {
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVDouble> TYPE = DataTypeForKV.from(PostgresDataType.FLOAT8, new DoubleValueConverter(), Types.DOUBLE);

    @Override
    public KVType getErasuredType() {
        return DoubleType.INSTANCE;
    }

    @Override
    public KVDouble from(Double databaseObject) {
        return KVDouble.of(databaseObject);
    }

    @Override
    public Double to(KVDouble userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<Double> fromType() {
        return Double.class;
    }

    @Override
    public Class<KVDouble> toType() {
        return KVDouble.class;
    }

    @Override
    public SqlBinding<Double> getSqlBinding() {
        return DoubleSqlBinding.INSTANCE;
    }

}
