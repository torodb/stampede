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

package com.torodb.packaging.config.model.protocol.mongo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.commands.pojos.index.type.IndexType;
import com.torodb.packaging.config.jackson.FilterListDeserializer;
import com.torodb.packaging.config.jackson.FilterListSerializer;
import com.torodb.packaging.config.model.protocol.mongo.FilterList.IndexFilter;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

@JsonSerialize(using = FilterListSerializer.class)
@JsonDeserialize(using = FilterListDeserializer.class)
public class FilterList extends HashMap<String, Map<String, List<IndexFilter>>> {

  private static final long serialVersionUID = -167780238191456194L;

  @JsonPropertyOrder({"name", "unique", "keys"})
  public static class IndexFilter {

    @JsonProperty(required = false)
    private String name = null;
    @JsonProperty(required = false)
    private Boolean unique = null;
    @NotNull
    @JsonProperty(required = true)
    private IndexFieldsFilter keys = new IndexFieldsFilter();

    public IndexFilter() {
    }

    public IndexFilter(String name, Boolean unique, Map<String, String> keys) {
      this.name = name;
      this.unique = unique;
      this.keys = keys != null ? new IndexFieldsFilter(keys) : new IndexFieldsFilter();
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Boolean getUnique() {
      return unique;
    }

    public void setUnique(Boolean unique) {
      this.unique = unique;
    }

    public IndexFieldsFilter getKeys() {
      return keys;
    }

    public void setKeys(IndexFieldsFilter keys) {
      this.keys = keys;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((keys == null) ? 0 : keys.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((unique == null) ? 0 : unique.hashCode());
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
      IndexFilter other = (IndexFilter) obj;
      if (keys == null) {
        if (other.keys != null) {
          return false;
        }
      } else if (!keys.equals(other.keys)) {
        return false;
      }
      if (name == null) {
        if (other.name != null) {
          return false;
        }
      } else if (!name.equals(other.name)) {
        return false;
      }
      if (unique == null) {
        if (other.unique != null) {
          return false;
        }
      } else if (!unique.equals(other.unique)) {
        return false;
      }
      return true;
    }
  }

  public static class IndexFieldsFilter extends LinkedHashMap<String, String> {

    private static final long serialVersionUID = -1571579992353508686L;

    public IndexFieldsFilter() {
      super();
    }

    public IndexFieldsFilter(Map<String, String> keys) {
      super(keys);
    }
  }

  public static IndexType getIndexType(String filterType) {
    for (KnownType knownType : KnownType.values()) {
      if (knownType.getIndexType().getName().equals(filterType)) {
        return knownType.getIndexType();
      }
    }

    throw new IllegalArgumentException("Unknown filter type " + filterType);
  }
}
