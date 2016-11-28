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

package com.torodb.backend;

import com.google.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.sql.Connection;

/**
 *
 */
public class DslContextFactoryImpl implements DslContextFactory {

  public final DataTypeProvider dataTypeProvider;

  @Inject
  public DslContextFactoryImpl(DataTypeProvider dataTypeProvider) {
    super();
    this.dataTypeProvider = dataTypeProvider;
  }

  @Override
  public DSLContext createDslContext(Connection connection) {
    return DSL.using(connection, dataTypeProvider.getDialect());
  }
}
