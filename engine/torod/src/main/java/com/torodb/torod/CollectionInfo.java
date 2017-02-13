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

import javax.annotation.Nonnull;
import javax.json.JsonObject;

/**
 *
 */
public class CollectionInfo {

  private static final String IS_CAPPED = "isCapped";
  private static final String MAX_IF_CAPPED = "maxIfCapped";

  private final String name;
  private final Type type;
  private final JsonObject properties;

  public CollectionInfo(@Nonnull String name, @Nonnull Type type,
      @Nonnull JsonObject properties) {
    this.name = name;
    this.type = type;
    this.properties = properties;
  }

  @Nonnull
  public String getName() {
    return name;
  }

  @Nonnull
  public String getType() {
    return type.getValue();
  }

  @Nonnull
  public JsonObject getProperties() {
    return properties;
  }

  public boolean isCapped() {
    return properties.containsKey(IS_CAPPED) && properties.getBoolean(IS_CAPPED);
  }

  public int getMaxIfCapped() {
    if (!properties.containsKey(MAX_IF_CAPPED)) {
      throw new IllegalStateException("The collection " + name + " has no " + MAX_IF_CAPPED
          + " property");
    }

    return properties.getInt(MAX_IF_CAPPED);
  }
  
  public static enum Type {
    COLLECTION("collection"),
    VIEW("view");
    
    public final String value;
    
    Type(String value) {
      this.value = value;
    }
    
    public String getValue() {
      return this.value;
    }
  }
}
