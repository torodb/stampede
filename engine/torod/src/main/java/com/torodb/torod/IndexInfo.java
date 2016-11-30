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

package com.torodb.torod;

import com.google.common.collect.ImmutableList;
import com.torodb.core.language.AttributeReference;

import javax.annotation.Nonnull;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 *
 */
public class IndexInfo {

  private final String name;
  private final boolean unique;
  private final JsonObject properties;
  private final ImmutableList<IndexFieldInfo> fields;

  public IndexInfo(@Nonnull String name, boolean unique, @Nonnull JsonObject properties,
      ImmutableList<IndexFieldInfo> fields) {
    this.name = name;
    this.unique = unique;
    this.properties = properties;
    this.fields = fields;
  }

  @Nonnull
  public String getName() {
    return name;
  }

  public boolean isUnique() {
    return unique;
  }

  @Nonnull
  public JsonObject getProperties() {
    return properties;
  }

  @Nonnull
  public ImmutableList<IndexFieldInfo> getFields() {
    return fields;
  }

  public static class Builder {

    private final String name;
    private final boolean isUnique;
    private final JsonObjectBuilder propertiesBuilder;
    private final ImmutableList.Builder<IndexFieldInfo> fieldsBuilder;

    public Builder(@Nonnull String name, boolean isUnique) {
      this.name = name;
      this.isUnique = isUnique;
      this.propertiesBuilder = Json.createObjectBuilder();
      this.fieldsBuilder = ImmutableList.builder();
    }

    public Builder addField(AttributeReference attributeReference, boolean isAscending) {
      fieldsBuilder.add(new IndexFieldInfo(attributeReference, isAscending));
      return this;
    }

    public IndexInfo build() {
      return new IndexInfo(name, isUnique,
          propertiesBuilder.build(),
          fieldsBuilder.build());
    }
  }

}
