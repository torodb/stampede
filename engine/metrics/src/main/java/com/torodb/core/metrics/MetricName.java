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

public class MetricName {

  private String group;
  private String type;
  private String name;
  private String mBeanName;

  public String getGroup() {
    return group;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public MetricName(Class<?> clasz, String name) {
    String group = "";
    if (clasz.getPackage() != null) {
      group = clasz.getPackage().getName();
    }
    String type = removeTailDollar(clasz.getSimpleName());
    build(group, type, name);
  }

  public MetricName(String group, String type, String name) {
    build(group, type, name);
  }

  public MetricName(String group, String type, String name, String mBeanName) {
    build(group, type, name, mBeanName);
  }

  private void build(String group, String type, String name) {
    String mBeanName = createMBeanName(group, type, name);
    build(group, type, name, mBeanName);
  }

  private void build(String group, String type, String name, String mBeanName) {
    if (group == null || type == null) {
      throw new IllegalArgumentException("Both group and type must be specified");
    }
    if (name == null) {
      throw new IllegalArgumentException("Name must be specified");
    }
    this.group = group;
    this.type = type;
    this.name = name;
    this.mBeanName = mBeanName;
  }

  public String getMetricName() {
    return MetricRegistry.name(group, type, name);
  }

  public ObjectName getMBeanName() {
    String mname = mBeanName;
    if (mname == null) {
      mname = getMetricName();
    }
    try {
      return new ObjectName(mname);
    } catch (MalformedObjectNameException e) {
      try {
        return new ObjectName(ObjectName.quote(mname));
      } catch (MalformedObjectNameException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricName other = (MetricName) o;
    return mBeanName.equals(other.mBeanName);
  }

  @Override
  public int hashCode() {
    return mBeanName.hashCode();
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

  private static String removeTailDollar(String s) {
    if (s.endsWith("$")) {
      return s.substring(0, s.length() - 1);
    }
    return s;
  }

}
