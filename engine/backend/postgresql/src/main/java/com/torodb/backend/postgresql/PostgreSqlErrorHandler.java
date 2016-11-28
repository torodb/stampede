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

package com.torodb.backend.postgresql;

import static com.torodb.backend.ErrorHandler.Context.ADD_COLUMN;
import static com.torodb.backend.ErrorHandler.Context.ADD_FOREIGN_KEY;
import static com.torodb.backend.ErrorHandler.Context.ADD_UNIQUE_INDEX;
import static com.torodb.backend.ErrorHandler.Context.CREATE_INDEX;
import static com.torodb.backend.ErrorHandler.Context.CREATE_SCHEMA;
import static com.torodb.backend.ErrorHandler.Context.CREATE_TABLE;
import static com.torodb.backend.ErrorHandler.Context.DROP_INDEX;
import static com.torodb.backend.ErrorHandler.Context.DROP_SCHEMA;
import static com.torodb.backend.ErrorHandler.Context.DROP_TABLE;
import static com.torodb.backend.ErrorHandler.Context.INSERT;
import static com.torodb.backend.ErrorHandler.Context.META_INSERT;

import com.torodb.backend.AbstractErrorHandler;
import com.torodb.core.exceptions.user.UniqueIndexViolationException;

/**
 *
 */
public class PostgreSqlErrorHandler extends AbstractErrorHandler {

  public PostgreSqlErrorHandler() {
    super(
        rollbackRule("40001"),
        rollbackRule("40P01"),
        /**
         * relation "?" already exists
         */
        rollbackRule("42P07"),
        /**
         * column "?" of relation "?" already exists
         */
        rollbackRule("42701"),
        /**
         * type "?" already exists
         */
        rollbackRule("42710"),
        /**
         * duplicate key value violates unique constraint "?"
         *
         * This will be raised when modifying DDL concurrently
         */
        rollbackRule("23505", CREATE_SCHEMA, CREATE_TABLE, ADD_UNIQUE_INDEX, ADD_FOREIGN_KEY,
            ADD_COLUMN, CREATE_INDEX, DROP_SCHEMA,
            DROP_TABLE, DROP_INDEX, META_INSERT),
        /**
         * Duplicate key value in unique index on insert
         */
        userRule("23505", b -> new UniqueIndexViolationException(b.getMessage(), b), INSERT)
    );
  }
}
