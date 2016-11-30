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

package com.torodb.backend.postgresql.meta;

import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.meta.AbstractSchemaUpdater;
import org.jooq.DSLContext;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PostgreSqlSchemaUpdater extends AbstractSchemaUpdater {

  @Inject
  public PostgreSqlSchemaUpdater(SqlInterface sqlInterface, SqlHelper sqlHelper) {
    super(sqlInterface, sqlHelper);
  }

  @Override
  protected void createSchema(DSLContext dsl, SqlInterface sqlInterface, SqlHelper sqlHelper) {
    super.createSchema(dsl, sqlInterface, sqlHelper);

    executeSql(dsl, "/sql/postgresql/mongo_timestamp_type.sql", sqlHelper);
  }
}
