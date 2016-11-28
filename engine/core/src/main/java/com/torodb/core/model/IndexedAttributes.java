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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.language.AttributeReference;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class IndexedAttributes implements Serializable {

  private static final long serialVersionUID = 1L;
  private final ImmutableList<AttributeReference> attributes;
  private final ImmutableMap<AttributeReference, IndexType> orderingInfo;

  private IndexedAttributes(
      ImmutableList<AttributeReference> attributes,
      ImmutableMap<AttributeReference, IndexType> attToIndex) {
    this.orderingInfo = attToIndex;
    this.attributes = attributes;
  }

  public List<AttributeReference> getIndexedAttributes() {
    return attributes;
  }

  public IndexType ascendingOrdered(AttributeReference attRef) {
    IndexType ascendigOrdered = orderingInfo.get(attRef);
    if (ascendigOrdered == null) {
      throw new IllegalArgumentException("Attribute " + attRef + " is not indexed by this index");
    }
    return ascendigOrdered;
  }

  public boolean contains(AttributeReference attRef) {
    return orderingInfo.containsKey(attRef);
  }

  public Iterable<Map.Entry<AttributeReference, IndexType>> entrySet() {
    return orderingInfo.entrySet();
  }

  public int size() {
    return attributes.size();
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 83 * hash + (this.orderingInfo != null ? this.orderingInfo.hashCode() : 0);
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
    final IndexedAttributes other = (IndexedAttributes) obj;
    if (this.attributes != other.attributes && (this.attributes == null || !this.attributes.equals(
        other.attributes))) {
      return false;
    }
    if (this.orderingInfo != other.orderingInfo && (this.orderingInfo == null || !this.orderingInfo
        .equals(other.orderingInfo))) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (AttributeReference attribute : attributes) {
      sb.append(attribute).append(' ');
      sb.append("(");
      sb.append(orderingInfo.get(attribute).name());
      sb.append(")");
      sb.append(", ");
    }
    sb.delete(sb.length() - 2, sb.length());

    return sb.toString();
  }

  public static class Builder {

    private final ImmutableList.Builder<AttributeReference> attributes;
    private final ImmutableMap.Builder<AttributeReference, IndexType> orderingInfo;

    public Builder() {
      attributes = ImmutableList.builder();
      orderingInfo = ImmutableMap.builder();
    }

    public Builder addAttribute(AttributeReference attRef, IndexType ascendingOrder) {
      attributes.add(attRef);
      orderingInfo.put(attRef, ascendingOrder);
      return this;
    }

    public IndexedAttributes build() {
      return new IndexedAttributes(
          attributes.build(),
          orderingInfo.build()
      );
    }
  }

  public enum IndexType {
    asc,
    desc,
    text,
    geospatial,
    hashed;
  }
}
