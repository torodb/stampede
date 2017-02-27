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

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TypeMetricNameFactory implements MetricNameFactory {

  public static final String GROUP_NAME = "com.torodb.metrics";

  private final String type;

  public TypeMetricNameFactory(String type) {
    this.type = type;
  }
  
  @Override
  public MetricNameFactory createSubFactory(String middleName) {
    return new SubMetricNameFactory(middleName, this);
  }

  @Override
  public MetricName createMetricName(Stream<String> names) {
    return new DefaultMetricName(
        GROUP_NAME,
        type,
        names.collect(Collectors.joining("."))
    );
  }
}
