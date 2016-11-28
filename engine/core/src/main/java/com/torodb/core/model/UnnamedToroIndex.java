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

import com.torodb.core.language.AttributeReference;
import com.torodb.core.model.IndexedAttributes.IndexType;

/**
 *
 */
public final class UnnamedToroIndex implements ToroIndex {

  private static final long serialVersionUID = 1L;

  private final String database;
  private final String collection;
  private final boolean unique;
  private final IndexedAttributes attributes;

  public UnnamedToroIndex(
      String database,
      String collection,
      boolean unique,
      IndexedAttributes attributes) {
    this.database = database;
    this.collection = collection;
    this.unique = unique;
    this.attributes = attributes;
  }

  @Override
  public String getDatabase() {
    return database;
  }

  @Override
  public String getCollection() {
    return collection;
  }

  @Override
  public boolean isUnique() {
    return unique;
  }

  @Override
  public IndexedAttributes getAttributes() {
    return attributes;
  }

  @Override
  public UnnamedToroIndex asUnnamed() {
    return this;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + (this.database != null ? this.database.hashCode() : 0);
    hash = 37 * hash + (this.collection != null ? this.collection.hashCode() : 0);
    hash = 37 * hash + (this.unique ? 1 : 0);
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
    final UnnamedToroIndex other = (UnnamedToroIndex) obj;
    if ((this.database == null) ? (other.database != null) :
        !this.database.equals(other.database)) {
      return false;
    }
    if ((this.collection == null) ? (other.collection != null) :
        !this.collection.equals(other.collection)) {
      return false;
    }
    if (this.unique != other.unique) {
      return false;
    }
    if (this.attributes != other.attributes && (this.attributes == null || !this.attributes.equals(
        other.attributes))) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "Toro index on (" + attributes + " (" + (unique ? "unique" : "no unique") + "))";
  }

  public static class Builder {

    private String name;
    private final IndexedAttributes.Builder attributesBuilder =
        new IndexedAttributes.Builder();
    private String database;
    private String collection;
    private boolean unique;

    public String getName() {
      return name;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public String getDatabaseName() {
      return database;
    }

    public Builder setDatabase(String database) {
      this.database = database;
      return this;
    }

    public String getCollection() {
      return collection;
    }

    public Builder setCollection(String collection) {
      this.collection = collection;
      return this;
    }

    public boolean isUnique() {
      return unique;
    }

    public Builder setUnique(boolean unique) {
      this.unique = unique;
      return this;
    }

    public Builder addIndexedAttribute(
        AttributeReference attRef,
        IndexType ascendingOrder) {
      this.attributesBuilder.addAttribute(attRef, ascendingOrder);
      return this;
    }

    public UnnamedToroIndex build() {
      return new UnnamedToroIndex(
          database,
          collection,
          unique,
          attributesBuilder.build()
      );
    }

  }

}
