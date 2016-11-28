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

package com.torodb.core;

import com.google.common.base.Objects;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public abstract class TableRef {

  public abstract Optional<TableRef> getParent();

  /**
   * The name of this TableRef on the document model.
   *
   * For example, the table referenced by "a.b.c" should have the name "c". On any collection, the
   * root TableRef has the empty name as string.
   *
   * @return
   */
  @Nonnull
  public abstract String getName();

  /**
   * The depth of this TableRef on the document model.
   *
   * For example, the table referenced by "a.b.c" should have depth 3. On any collection, the root
   * TableRef has the depth 0.
   *
   * @return
   */
  @Nonnull
  public abstract int getDepth();

  /**
   * The array dimension of this TableRef on the document model if the array dimension is greather
   * than 2 or 0 otherwise.
   *
   * For example, the table referenced by "a.b.c.$2.$3" should have array dimension 3. On any
   * collection, the root TableRef has array dimension 0.
   *
   * @return
   */
  @Nonnull
  public abstract int getArrayDimension();

  /**
   * Indicates if this TableRef has is contained by an array.
   *
   * @return
   */
  @Nonnull
  public abstract boolean isInArray();

  public boolean isRoot() {
    return !getParent().isPresent();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    toString(sb);

    return sb.toString();
  }

  protected void toString(StringBuilder sb) {
    Optional<TableRef> parent = getParent();
    if (parent.isPresent()) {
      TableRef parentRef = parent.get();
      parentRef.toString(sb);
      if (!parentRef.isRoot()) {
        sb.append('.');
      }
    }
    sb.append(getName());
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof TableRef)) {
      return false;
    }
    TableRef otherRef = (TableRef) other;

    return getName().equals(otherRef.getName()) && Objects.equal(getParent(), otherRef.getParent());
  }

}
