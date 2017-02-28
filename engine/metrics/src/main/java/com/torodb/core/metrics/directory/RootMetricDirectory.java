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

package com.torodb.core.metrics.directory;

import com.torodb.core.metrics.Hierarchy;
import com.torodb.core.metrics.Name;

public final class RootMetricDirectory implements Directory {

  @Override
  public Directory createDirectory(String key, String name) {
    return new DefaultMetricDirectory(new Hierarchy(key, name));
  }

  @Override
  public Directory createDirectory(String name) {
    return new DefaultMetricDirectory(new Hierarchy("type", name));
  }

  @Override
  public Name createName(String key, String name) {
    return new Name(new Hierarchy(key, name));
  }

  @Override
  public Name createName(String name) {
    return new Name(new Hierarchy("type", name));
  }

}
