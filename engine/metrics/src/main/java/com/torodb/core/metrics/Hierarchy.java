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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.Immutable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

@Immutable
public class Hierarchy {

  private static final String GROUP_NAME = "com.torodb.metrics";

  private final ImmutableList<Map.Entry<String, String>> list;

  public Hierarchy(ImmutableList<Map.Entry<String, String>> list) {
    this.list = list;
    list.stream()
        .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
        .forEach(Hierarchy::checkName);
  }

  public Hierarchy(String firstKey, String firstValue) {
    checkName(firstKey);
    checkName(firstValue);

    this.list = ImmutableList.of(new AbstractMap.SimpleEntry<>(firstKey, firstValue));
  }

  public Hierarchy(Hierarchy parent, String key, String value) {
    checkName(key);
    checkName(value);

    this.list = ImmutableList.copyOf(
        Iterables.concat(
            parent.asList(),
            Collections.singleton(new AbstractMap.SimpleEntry<>(key, value))
        )
    );
  }

  public static Hierarchy empty() {
    return new Hierarchy(ImmutableList.of());
  }

  public final List<Map.Entry<String, String>> asList() {
    return list;
  }

  public String getQualifiedName() {
    if (list.isEmpty()) {
      throw new IllegalStateException("The root hierarchy does not have a qualified name");
    }
    return list.stream()
        .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
        .collect(Collectors.joining("."));
  }

  public ObjectName getMBeanName() {
    if (list.isEmpty()) {
      throw new IllegalStateException("The root hierarchy does not have a MBean name");
    }

    String mname = createMBeanName(asList());
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

  private static String createMBeanName(List<Map.Entry<String, String>> list) {
    assert !list.isEmpty();

    StringBuilder nameBuilder = new StringBuilder();
    nameBuilder.append(GROUP_NAME)
        .append(':');

    for (Map.Entry<String, String> entry : list) {
      nameBuilder.append(entry.getKey())
          .append('=')
          .append(entry.getValue())
          .append(',');
    }

    nameBuilder.deleteCharAt(nameBuilder.length() - 1);
    
    return nameBuilder.toString();
  }

  public static void checkName(String name) {

    if (name == null) {
      throw new IllegalArgumentException("Names must be not null");
    }
    if (name.isEmpty()) {
      throw new IllegalArgumentException("Names must be not empty");
    }

    Integer invalidCodePoint = name.codePoints()
        .filter(cp -> !Character.isJavaIdentifierPart(cp))
        .filter(cp -> cp != '.')
        .boxed()
        .findAny()
        .orElse(null);

    if (invalidCodePoint != null) {
      String invalidText = new String(Character.toChars(invalidCodePoint));
      throw new IllegalArgumentException("The name " + name + " contains the illegal "
          + "character " + invalidText
      );
    }
  }

}
