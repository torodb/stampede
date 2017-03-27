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

package com.torodb.core.metrics;

import java.util.Objects;

import javax.management.ObjectName;

public final class Name {

  private final String qualifiedName;
  private final ObjectName mBeanName;

  public Name(Hierarchy hierarchy) {
    qualifiedName = hierarchy.getQualifiedName();
    mBeanName = hierarchy.getMBeanName();
  }

  public String getQualifiedName() {
    return qualifiedName;
  }

  public ObjectName getMBeanName() {
    return mBeanName;
  }

  @Override
  public String toString() {
    return mBeanName.toString();
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + Objects.hashCode(this.qualifiedName);
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
    final Name other = (Name) obj;
    if (!Objects.equals(this.qualifiedName, other.qualifiedName)) {
      return false;
    }
    return true;
  }

}
