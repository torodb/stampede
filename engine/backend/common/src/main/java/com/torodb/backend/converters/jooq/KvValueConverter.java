/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.converters.jooq;

import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvValue;
import org.jooq.Converter;

/**
 *
 * @param <DatabaseTypeT> data base type
 * @param <JavaTypeT>  an intermediate JDBC-friendly Java type
 * @param <V>   a value of the given {@code KvValue} type
 */
public interface KvValueConverter<DatabaseTypeT, JavaTypeT, V extends KvValue<?>>
    extends Converter<DatabaseTypeT, V> {

  public KvType getErasuredType();

  public SqlBinding<JavaTypeT> getSqlBinding();
}
