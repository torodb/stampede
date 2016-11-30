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

package com.torodb.kvdocument.values;

public interface KvValueVisitor<R, A> {

  public R visit(KvBoolean value, A arg);

  public R visit(KvNull value, A arg);

  public R visit(KvArray value, A arg);

  public R visit(KvInteger value, A arg);

  public R visit(KvLong value, A arg);

  public R visit(KvDouble value, A arg);

  public R visit(KvString value, A arg);

  public R visit(KvDocument value, A arg);

  public R visit(KvMongoObjectId value, A arg);

  public R visit(KvInstant value, A arg);

  public R visit(KvDate value, A arg);

  public R visit(KvTime value, A arg);

  public R visit(KvBinary value, A arg);

  public R visit(KvMongoTimestamp value, A arg);

}
