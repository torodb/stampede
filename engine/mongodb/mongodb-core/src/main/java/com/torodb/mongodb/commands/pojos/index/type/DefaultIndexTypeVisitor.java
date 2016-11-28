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

package com.torodb.mongodb.commands.pojos.index.type;

public abstract class DefaultIndexTypeVisitor<A, R> implements IndexTypeVisitor<A, R> {

  protected abstract R defaultVisit(IndexType indexType, A arg);

  @Override
  public R visit(AscIndexType indexType, A arg) {
    return defaultVisit(indexType, arg);
  }

  @Override
  public R visit(DescIndexType indexType, A arg) {
    return defaultVisit(indexType, arg);
  }

  @Override
  public R visit(TextIndexType indexType, A arg) {
    return defaultVisit(indexType, arg);
  }

  @Override
  public R visit(HashedIndexType indexType, A arg) {
    return defaultVisit(indexType, arg);
  }

  @Override
  public R visit(TwoDIndexType indexType, A arg) {
    return defaultVisit(indexType, arg);
  }

  @Override
  public R visit(TwoDSphereIndexType indexType, A arg) {
    return defaultVisit(indexType, arg);
  }

  @Override
  public R visit(GeoHaystackIndexType indexType, A arg) {
    return defaultVisit(indexType, arg);
  }

  @Override
  public R visit(UnknownIndexType indexType, A arg) {
    return defaultVisit(indexType, arg);
  }

}
