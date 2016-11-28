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

package com.torodb.core.model;

/**
 *
 */
public class DefaultNamedToroIndex implements NamedToroIndex {

  private static final long serialVersionUID = 1L;

  private final String name;
  private final UnnamedToroIndex unnamedIndex;

  public DefaultNamedToroIndex(
      String name,
      IndexedAttributes attributes,
      String databaseName,
      String collection,
      boolean unique) {
    this.name = name;
    this.unnamedIndex = new UnnamedToroIndex(databaseName, collection, unique, attributes);
  }

  public DefaultNamedToroIndex(String name, UnnamedToroIndex unnamedIndex) {
    this.name = name;
    this.unnamedIndex = unnamedIndex;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public UnnamedToroIndex asUnnamed() {
    return unnamedIndex;
  }

  @Override
  public String getDatabase() {
    return unnamedIndex.getDatabase();
  }

  @Override
  public String getCollection() {
    return unnamedIndex.getCollection();
  }

  @Override
  public boolean isUnique() {
    return unnamedIndex.isUnique();
  }

  @Override
  public IndexedAttributes getAttributes() {
    return unnamedIndex.getAttributes();
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 47 * hash + (this.name != null ? this.name.hashCode() : 0);
    hash = 47 * hash + (this.unnamedIndex != null ? this.unnamedIndex.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final DefaultNamedToroIndex other = (DefaultNamedToroIndex) obj;
    if ((this.name == null) ? (other.name != null) :
        !this.name.equals(other.name)) {
      return false;
    }
    if (this.unnamedIndex != other.unnamedIndex && (this.unnamedIndex == null || !this.unnamedIndex
        .equals(other.unnamedIndex))) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return name;
  }
}
