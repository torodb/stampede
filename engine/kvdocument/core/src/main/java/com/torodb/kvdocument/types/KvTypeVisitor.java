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

package com.torodb.kvdocument.types;

public interface KvTypeVisitor<R, A> {

  public R visit(ArrayType type, A arg);

  public R visit(BooleanType type, A arg);

  public R visit(DoubleType type, A arg);

  public R visit(IntegerType type, A arg);

  public R visit(LongType type, A arg);

  public R visit(NullType type, A arg);

  public R visit(DocumentType type, A arg);

  public R visit(StringType type, A arg);

  public R visit(GenericType type, A arg);

  public R visit(MongoObjectIdType type, A arg);

  public R visit(InstantType type, A arg);

  public R visit(DateType type, A arg);

  public R visit(TimeType type, A arg);

  public R visit(BinaryType type, A arg);

  public R visit(NonExistentType type, A arg);

  public R visit(MongoTimestampType type, A arg);

}
