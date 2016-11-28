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

package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.DateType;

import java.time.LocalDate;

public abstract class KvDate extends KvValue<LocalDate> {

  private static final long serialVersionUID = -218788179965687517L;

  @Override
  public DateType getType() {
    return DateType.INSTANCE;
  }

  @Override
  public Class<? extends LocalDate> getValueClass() {
    return LocalDate.class;
  }

  @Override
  public String toString() {
    return getValue().toString();
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvDate)) {
      return false;
    }
    KvDate other = (KvDate) obj;
    return this.getValue().equals(other.getValue());
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }
}
