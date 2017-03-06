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

package com.torodb.core.logging;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

public class DefaultToroMessageFactory extends ToroMessageFactory {

  private static final long serialVersionUID = 5492349029340L;

  private final SortedMap<String, String> context;

  public DefaultToroMessageFactory(String component) {
    this(component, new TreeMap<>());
  }

  public DefaultToroMessageFactory(String component, SortedMap<String, String> context) {
    context.put("component", component);
    this.context = Collections.unmodifiableSortedMap(context);
  }

  @Override
  protected SortedMap<String, String> newContextMap() {
    return context;
  }

}
