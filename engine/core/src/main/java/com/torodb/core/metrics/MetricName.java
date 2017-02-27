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

import javax.management.ObjectName;

public interface MetricName {

  public String getMetricName();

  public ObjectName getMBeanName();

  public default boolean defaultEquals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || !(other instanceof MetricName)) {
      return false;
    }
    return this.getMBeanName().equals(((MetricName) other).getMBeanName());
  }

  public default int defaultHashCode() {
    return getMBeanName().hashCode();
  }

}
