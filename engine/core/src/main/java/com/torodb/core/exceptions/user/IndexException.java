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

import javax.annotation.Nullable;

public abstract class IndexException extends UserException {

  private static final long serialVersionUID = 1L;

  @Nullable
  private final String database;
  @Nullable
  private final String collection;
  @Nullable
  private final String index;

  protected IndexException(String database, String collection, String index) {
    super();
    this.database = database;
    this.collection = collection;
    this.index = index;
  }

  protected IndexException(String message, String database, String collection, String index) {
    super(message);
    this.database = database;
    this.collection = collection;
    this.index = index;
  }

  protected IndexException(Throwable cause, String database, String collection, String index) {
    super(cause);
    this.database = database;
    this.collection = collection;
    this.index = index;
  }

  protected IndexException(String message, Throwable cause, String database, String collection,
      String index) {
    super(message, cause);
    this.database = database;
    this.collection = collection;
    this.index = index;
  }

  @Nullable
  public String getDatabase() {
    return database;
  }

  @Nullable
  public String getCollection() {
    return collection;
  }

  @Nullable
  public String getIndex() {
    return index;
  }

}
