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

package com.torodb.backend.postgresql;

import static com.torodb.backend.ErrorHandler.Context.*;
import com.torodb.backend.AbstractErrorHandler;

/**
 *
 */
public class PostgreSQLErrorHandler extends AbstractErrorHandler {
    public PostgreSQLErrorHandler() {
        super(
                rule("40001"), 
                rule("40P01"),
                /**
                 * relation "?" already exists
                 */
                rule("42P07"),
                /**
                 * column "?" of relation "?" already exists
                 */
                rule("42701"),
                /**
                 * type "?" already exists
                 */
                rule("42710"),
                /**
                 * duplicate key value violates unique constraint "?"
                 * 
                 * This will be raised when modifying DDL concurrently
                 */
                rule("23505", CREATE_SCHEMA, CREATE_TABLE, ADD_COLUMN, CREATE_INDEX, DROP_SCHEMA, DROP_TABLE, DROP_INDEX)
                );
    }
}
