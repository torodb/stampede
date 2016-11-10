/*
 * MongoWP - ToroDB-poc: Backend common
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

package com.torodb.backend.converters.jooq;

import org.jooq.Converter;

import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVValue;

/**
 *
 * @param <DBT> data base type
 * @param <JT> an intermediate JDBC-friendly Java type
 * @param <V> a value of the given {@code KVValue} type
 */
public interface KVValueConverter<DBT, JT, V extends KVValue<?>> extends Converter<DBT, V> {
    
    public KVType getErasuredType();
    
    public SqlBinding<JT> getSqlBinding();
}
