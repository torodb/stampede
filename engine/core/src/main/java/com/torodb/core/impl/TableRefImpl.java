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

package com.torodb.core.impl;

import com.torodb.core.TableRef;

import java.util.Optional;

import javax.annotation.Nonnull;

public class TableRefImpl extends TableRef {

  protected static final TableRef ROOT = new TableRefImpl();

  private final Optional<TableRef> parent;
  private final String name;
  private final int depth;
  private final int arrayDimension;

  protected TableRefImpl() {
    super();
    this.parent = Optional.empty();
    this.name = "";
    this.depth = 0;
    this.arrayDimension = 0;
  }

  protected TableRefImpl(@Nonnull TableRef parent, @Nonnull String name) {
    super();
    this.parent = Optional.of(parent);
    this.name = name;
    this.depth = parent.getDepth() + 1;
    this.arrayDimension = 0;
  }

  protected TableRefImpl(@Nonnull TableRef parent, int arrayDimension) {
    super();

    if (arrayDimension < 2) {
      throw new IllegalArgumentException("array dimension should be greather than 1");
    } else {
      if (arrayDimension > 2 && parent.getArrayDimension() + 1 != arrayDimension) {
        throw new IllegalArgumentException("array dimension should be " + (parent
            .getArrayDimension() + 1));
      }
    }

    this.parent = Optional.of(parent);
    this.name = ("$" + arrayDimension).intern();
    this.depth = parent.getDepth() + 1;
    this.arrayDimension = arrayDimension;
  }

  @Override
  public boolean isInArray() {
    return arrayDimension > 1;
  }

  @Override
  public Optional<TableRef> getParent() {
    return parent;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getDepth() {
    return depth;
  }

  @Override
  public int getArrayDimension() {
    return arrayDimension;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + arrayDimension;
    result = prime * result + depth;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((parent == null) ? 0 : parent.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TableRefImpl other = (TableRefImpl) obj;
    if (arrayDimension != other.arrayDimension) {
      return false;
    }
    if (depth != other.depth) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (parent == null) {
      if (other.parent != null) {
        return false;
      }
    } else if (!parent.equals(other.parent)) {
      return false;
    }
    return true;
  }
}
