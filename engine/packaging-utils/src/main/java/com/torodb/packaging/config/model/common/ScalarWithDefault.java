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

package com.torodb.packaging.config.model.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class ScalarWithDefault<T> {

  public static <S extends ScalarWithDefault<?>> S merge(S specific, S common) {
    if (specific.isDefault()) {
      return common;
    }
    
    return specific;
  }

  private final T value;
  private final boolean isDefault;
  
  public ScalarWithDefault(T value, boolean isDefault) {
    super();
    this.value = value;
    this.isDefault = isDefault;
  }
  
  @JsonProperty(value = "value")
  public T value() {
    return value;
  }

  @JsonIgnore
  public boolean hasValue() {
    return value != null;
  }
  
  @JsonIgnore
  public T mergeValue(ScalarWithDefault<T> common) {
    if (isDefault()) {
      return common.value();
    }
    
    return value();
  }

  @JsonProperty(value = "default")
  public boolean isDefault() {
    return isDefault;
  }

  @JsonIgnore
  public boolean notDefault() {
    return !isDefault;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (isDefault ? 1231 : 1237);
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
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
    ScalarWithDefault<?> other = (ScalarWithDefault<?>) obj;
    if (isDefault != other.isDefault) {
      return false;
    }
    if (value == null) {
      if (other.value != null) {
        return false;
      }
    } else if (!value.equals(other.value)) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
    return "{" + "value: " + value + ", default: " + isDefault + "}";
  }
}
