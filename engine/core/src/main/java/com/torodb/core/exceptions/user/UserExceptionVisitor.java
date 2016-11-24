/*
 * ToroDB - ToroDB: Core
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
package com.torodb.core.exceptions.user;

public interface UserExceptionVisitor<Result, Argument> {
    public Result visit(DatabaseNotFoundException userException, Argument arg);
    public Result visit(CollectionNotFoundException userException, Argument arg);
    public Result visit(IndexNotFoundException userException, Argument arg);
    public Result visit(UnsupportedUniqueIndexException userException, Argument arg);
    public Result visit(UpdateException userException, Argument arg);
    public Result visit(UniqueIndexViolationException userException, Argument arg);
}