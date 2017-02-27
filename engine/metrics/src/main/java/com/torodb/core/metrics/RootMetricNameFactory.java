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

import java.util.stream.Stream;

public final class RootMetricNameFactory implements MetricNameFactory {

  private final MetricNameFactory defaultFactory;

  public RootMetricNameFactory() {
    defaultFactory = createSubFactory("root");
  }

  @Override
  public MetricNameFactory createSubFactory(String middleName) {
    return new TypeMetricNameFactory(middleName);
  }

  @Override
  public MetricName createMetricName(Stream<String> names) {
    return defaultFactory.createMetricName(names);
  }

  @Override
  public MetricName createMetricName(String name) {
    return defaultFactory.createMetricName(name);
  }

  @Override
  public MetricName createMetricName(String... names) {
    return defaultFactory.createMetricName(names);
  }

}
