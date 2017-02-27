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

import com.codahale.metrics.MetricRegistry;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

final class DefaultMetricName implements MetricName {

  private final String group;
  private final String type;
  private final String name;
  private final String mBeanName;

  public DefaultMetricName(String group, String type, String name) {
    if (group == null || type == null) {
      throw new IllegalArgumentException("Both group and type must be specified");
    }
    if (name == null) {
      throw new IllegalArgumentException("Name must be specified");
    }
    this.mBeanName = createMBeanName(group, type, name);
    this.group = group;
    this.type = type;
    this.name = name;
  }

  public String getGroup() {
    return group;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  @Override
  public String getMetricName() {
    return MetricRegistry.name(group, type, name);
  }

  @Override
  public ObjectName getMBeanName() {
    String mname = mBeanName;
    try {
      return new ObjectName(mname);
    } catch (MalformedObjectNameException ex) {
      try {
        return new ObjectName(ObjectName.quote(mname));
      } catch (MalformedObjectNameException ex2) {
        throw new RuntimeException(ex2);
      }
    }
  }

  @Override
  public boolean equals(Object other) {
    return defaultEquals(other);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public String toString() {
    return mBeanName;
  }

  private String createMBeanName(String group, String type, String name) {
    StringBuilder nameBuilder = new StringBuilder();
    nameBuilder.append(group);
    nameBuilder.append(":type=");
    nameBuilder.append(type);
    if (!name.isEmpty()) {
      nameBuilder.append(",name=");
      nameBuilder.append(name);
    }
    return nameBuilder.toString();
  }

}
