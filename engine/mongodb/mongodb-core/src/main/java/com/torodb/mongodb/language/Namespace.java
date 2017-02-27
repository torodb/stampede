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

package com.torodb.mongodb.language;

import java.util.Objects;

public class Namespace {
  private final String database;
  private final String collection;

  public Namespace(String database, String collection) {
    this.database = database;
    this.collection = collection;
  }

  public String getDatabase() {
    return database;
  }

  public String getCollection() {
    return collection;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 13 * hash + Objects.hashCode(this.database);
    hash = 13 * hash + Objects.hashCode(this.collection);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Namespace other = (Namespace) obj;
    if (!Objects.equals(this.database, other.database)) {
      return false;
    }
    if (!Objects.equals(this.collection, other.collection)) {
      return false;
    }
    return true;
  }

}
