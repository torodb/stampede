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

package com.torodb.core.exceptions.user;

public interface UserExceptionVisitor<R, A> {

  public R visit(DatabaseNotFoundException userException, A arg);

  public R visit(CollectionNotFoundException userException, A arg);

  public R visit(IndexNotFoundException userException, A arg);

  public R visit(UnsupportedUniqueIndexException userException, A arg);

  public R visit(UnsupportedCompoundIndexException userException, A arg);

  public R visit(UpdateException userException, A arg);

  public R visit(UniqueIndexViolationException userException, A arg);
}
